#include <Storages/MergeTree/MutateTask.h>

#include <DataTypes/ObjectUtils.h>
#include <IO/HashingWriteBuffer.h>
#include <Common/logger_useful.h>
#include <Common/escapeForFileName.h>
#include <Core/Settings.h>
#include <Storages/MergeTree/DataPartStorageOnDiskFull.h>
#include <Storages/Statistics/Statistics.h>
#include <Columns/ColumnsNumber.h>
#include <Parsers/ASTAssignment.h>
#include <Parsers/queryToString.h>
#include <Interpreters/Squashing.h>
#include <Interpreters/MergeTreeTransaction.h>
#include <Interpreters/PreparedSets.h>
#include <Processors/Transforms/TTLTransform.h>
#include <Processors/Transforms/TTLCalcTransform.h>
#include <Processors/Transforms/DistinctSortedTransform.h>
#include <Processors/Transforms/ColumnGathererTransform.h>
#include <Processors/Sources/SourceFromSingleChunk.h>
#include <Processors/Transforms/ExpressionTransform.h>
#include <Processors/Transforms/MaterializingTransform.h>
#include <Processors/Executors/PullingPipelineExecutor.h>
#include <Processors/QueryPlan/CreatingSetsStep.h>
#include <Processors/QueryPlan/Optimizations/QueryPlanOptimizationSettings.h>
#include <Storages/MergeTree/StorageFromMergeTreeDataPart.h>
#include <Storages/MergeTree/MergeTreeDataWriter.h>
#include <Storages/MutationCommands.h>
#include <Storages/MergeTree/MergeTreeDataMergerMutator.h>
#include <Storages/MergeTree/MergeTreeIndexFullText.h>
#include <Storages/MergeTree/MergeTreeSettings.h>
#include <Storages/MergeTree/MergeTreeVirtualColumns.h>
#include <DataTypes/DataTypeNullable.h>
#include <DataTypes/DataTypeVariant.h>
#include <boost/algorithm/string/replace.hpp>
#include <Common/ProfileEventsScope.h>
#include <Core/ColumnsWithTypeAndName.h>


#if USE_TANTIVY_SEARCH
#    include <Storages/MergeTree/TantivyIndexStoreFactory.h>
#endif
#include <VectorIndex/Common/VICommon.h>
#include <VectorIndex/Common/SegmentsMgr.h>
#include <VectorIndex/Utils/VIUtils.h>


namespace ProfileEvents
{
    extern const Event MutationTotalParts;
    extern const Event MutationUntouchedParts;
    extern const Event MutationTotalMilliseconds;
    extern const Event MutationExecuteMilliseconds;
    extern const Event MutationAllPartColumns;
    extern const Event MutationSomePartColumns;
    extern const Event MutateTaskProjectionsCalculationMicroseconds;
}

namespace CurrentMetrics
{
    extern const Metric PartMutation;
}

namespace DB
{

namespace ErrorCodes
{
    extern const int ABORTED;
    extern const int LOGICAL_ERROR;
}

namespace MutationHelpers
{

static bool checkOperationIsNotCanceled(ActionBlocker & merges_blocker, MergeListEntry * mutate_entry)
{
    if (merges_blocker.isCancelled() || (*mutate_entry)->is_cancelled)
        throw Exception(ErrorCodes::ABORTED, "Cancelled mutating parts");

    return true;
}

static bool haveMutationsOfDynamicColumns(const MergeTreeData::DataPartPtr & data_part, const MutationCommands & commands)
{
    for (const auto & command : commands)
    {
        if (!command.column_name.empty())
        {
            auto column = data_part->tryGetColumn(command.column_name);
            if (column && column->type->hasDynamicSubcolumns())
                return true;
        }
    }

    return false;
}

static UInt64 getExistingRowsCount(const Block & block, bool lightweight_delete_updated, UInt64 & part_offset, std::vector<UInt64> & del_row_ids)
{
    auto column = block.getByName(RowExistsColumn::name).column;
    const ColumnUInt8 * row_exists_col = typeid_cast<const ColumnUInt8 *>(column.get());

    if (!row_exists_col)
    {
        LOG_WARNING(getLogger("MutationHelpers::getExistingRowsCount"), "_row_exists column type is not UInt8");
        return block.rows();
    }

    UInt64 existing_count = 0;

    for (UInt8 row_exists : row_exists_col->getData())
    {
        if (row_exists)
            existing_count++;

        /// Collect deleted row ids when LWD happens
        if (lightweight_delete_updated)
        {
            if (!row_exists)
                del_row_ids.emplace_back(part_offset);
            part_offset++;
        }
    }

    return existing_count;
}

/** Split mutation commands into two parts:
*   First part should be executed by mutations interpreter.
*   Other is just simple drop/renames, so they can be executed without interpreter.
*/
static void splitAndModifyMutationCommands(
    MergeTreeData::DataPartPtr part,
    StorageMetadataPtr metadata_snapshot,
    const MutationCommands & commands,
    MutationCommands & for_interpreter,
    MutationCommands & for_file_renames,
    LoggerPtr log)
{
    auto part_columns = part->getColumnsDescription();
    const auto & table_columns = metadata_snapshot->getColumns();

    if (haveMutationsOfDynamicColumns(part, commands) || !isWidePart(part) || !isFullPartStorage(part->getDataPartStorage()))
    {
        NameSet mutated_columns;
        NameSet dropped_columns;

        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            {
                /// For ordinary column with default or materialized expression, MATERIALIZE COLUMN should not override past values
                /// So we only mutate column if `command.column_name` is a default/materialized column or if the part does not have physical column file
                auto column_ordinary = table_columns.getOrdinary().tryGetByName(command.column_name);
                if (!column_ordinary || !part->tryGetColumn(command.column_name) || !part->hasColumnFiles(*column_ordinary))
                {
                    for_interpreter.push_back(command);
                    mutated_columns.emplace(command.column_name);
                }
            }
            if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_STATISTICS
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::UPDATE
                || command.type == MutationCommand::Type::APPLY_DELETED_MASK)
            {
                for_interpreter.push_back(command);
                for (const auto & [column_name, expr] : command.column_to_update_expression)
                    mutated_columns.emplace(column_name);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX
                     || command.type == MutationCommand::Type::DROP_PROJECTION
                     || command.type == MutationCommand::Type::DROP_STATISTICS)
            {
                for_file_renames.push_back(command);
            }
            else if (bool has_column = part_columns.has(command.column_name), has_nested_column = part_columns.hasNested(command.column_name); has_column || has_nested_column)
            {
                if (command.type == MutationCommand::Type::DROP_COLUMN || command.type == MutationCommand::Type::RENAME_COLUMN)
                {
                    if (has_nested_column)
                    {
                        const auto & nested = part_columns.getNested(command.column_name);
                        assert(!nested.empty());
                        for (const auto & nested_column : nested)
                            mutated_columns.emplace(nested_column.name);
                    }
                    else
                        mutated_columns.emplace(command.column_name);

                    if (command.type == MutationCommand::Type::DROP_COLUMN)
                        dropped_columns.emplace(command.column_name);
                }
            }

        }

        auto alter_conversions = part->storage.getAlterConversionsForPart(part);

        /// We don't add renames from commands, instead we take them from rename_map.
        /// It's important because required renames depend not only on part's data version (i.e. mutation version)
        /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
        /// can be deduced based on difference between part's schema and table schema.
        for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
        {
            if (part_columns.has(rename_from))
            {
                /// Actual rename
                for_interpreter.push_back(
                {
                    .type = MutationCommand::Type::READ_COLUMN,
                    .column_name = rename_to,
                });

                /// Not needed for compact parts (not executed), added here only to produce correct
                /// set of columns for new part and their serializations
                for_file_renames.push_back(
                {
                     .type = MutationCommand::Type::RENAME_COLUMN,
                     .column_name = rename_from,
                     .rename_to = rename_to
                });

                part_columns.rename(rename_from, rename_to);
            }
        }

        /// If it's compact part, then we don't need to actually remove files
        /// from disk we just don't read dropped columns
        for (const auto & column : part_columns)
        {
            if (!mutated_columns.contains(column.name))
            {
                if (!metadata_snapshot->getColumns().has(column.name) && !part->storage.getVirtualsPtr()->has(column.name))
                {
                    /// We cannot add the column because there's no such column in table.
                    /// It's okay if the column was dropped. It may also absent in dropped_columns
                    /// if the corresponding MUTATE_PART entry was not created yet or was created separately from current MUTATE_PART.
                    /// But we don't know for sure what happened.
                    auto part_metadata_version = part->getMetadataVersion();
                    auto table_metadata_version = metadata_snapshot->getMetadataVersion();

                    bool allow_equal_versions = part_metadata_version == table_metadata_version && part->old_part_with_no_metadata_version_on_disk;
                    if (part_metadata_version < table_metadata_version || allow_equal_versions)
                    {
                        LOG_WARNING(log, "Ignoring column {} from part {} with metadata version {} because there is no such column "
                                         "in table {} with metadata version {}. Assuming the column was dropped", column.name, part->name,
                                    part_metadata_version, part->storage.getStorageID().getNameForLogs(), table_metadata_version);
                        continue;
                    }

                    /// StorageMergeTree does not have metadata version
                    if (part->storage.supportsReplication())
                        throw Exception(ErrorCodes::LOGICAL_ERROR, "Part {} with metadata version {} contains column {} that is absent "
                                        "in table {} with metadata version {}",
                                        part->name, part_metadata_version, column.name,
                                        part->storage.getStorageID().getNameForLogs(), table_metadata_version);
                }

                for_interpreter.emplace_back(
                    MutationCommand{.type = MutationCommand::Type::READ_COLUMN, .column_name = column.name, .data_type = column.type});
            }
            else if (dropped_columns.contains(column.name))
            {
                /// Not needed for compact parts (not executed), added here only to produce correct
                /// set of columns for new part and their serializations
                for_file_renames.push_back(
                {
                     .type = MutationCommand::Type::DROP_COLUMN,
                     .column_name = column.name,
                });
            }
        }
    }
    else
    {
        for (const auto & command : commands)
        {
            if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            {
                /// For ordinary column with default or materialized expression, MATERIALIZE COLUMN should not override past values
                /// So we only mutate column if `command.column_name` is a default/materialized column or if the part does not have physical column file
                auto column_ordinary = table_columns.getOrdinary().tryGetByName(command.column_name);
                if (!column_ordinary || !part->tryGetColumn(command.column_name) || !part->hasColumnFiles(*column_ordinary))
                    for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::MATERIALIZE_INDEX
                || command.type == MutationCommand::Type::MATERIALIZE_STATISTICS
                || command.type == MutationCommand::Type::MATERIALIZE_PROJECTION
                || command.type == MutationCommand::Type::MATERIALIZE_TTL
                || command.type == MutationCommand::Type::DELETE
                || command.type == MutationCommand::Type::LIGHTWEIGHT_DELETE
                || command.type == MutationCommand::Type::UPDATE
                || command.type == MutationCommand::Type::APPLY_DELETED_MASK)
            {
                for_interpreter.push_back(command);
            }
            else if (command.type == MutationCommand::Type::DROP_INDEX
                     || command.type == MutationCommand::Type::DROP_PROJECTION
                     || command.type == MutationCommand::Type::DROP_STATISTICS)
            {
                for_file_renames.push_back(command);
            }
            /// If we don't have this column in source part, we don't need to materialize it.
            else if (part_columns.has(command.column_name))
            {
                if (command.type == MutationCommand::Type::READ_COLUMN)
                    for_interpreter.push_back(command);
                else if (command.type == MutationCommand::Type::RENAME_COLUMN)
                    part_columns.rename(command.column_name, command.rename_to);

                for_file_renames.push_back(command);
            }
        }

        auto alter_conversions = part->storage.getAlterConversionsForPart(part);
        /// We don't add renames from commands, instead we take them from rename_map.
        /// It's important because required renames depend not only on part's data version (i.e. mutation version)
        /// but also on part's metadata version. Why we have such logic only for renames? Because all other types of alter
        /// can be deduced based on difference between part's schema and table schema.

        for (const auto & [rename_to, rename_from] : alter_conversions->getRenameMap())
        {
            for_file_renames.push_back({.type = MutationCommand::Type::RENAME_COLUMN, .column_name = rename_from, .rename_to = rename_to});
        }
    }
}

/// Get the columns list of the resulting part in the same order as storage_columns.
static std::pair<NamesAndTypesList, SerializationInfoByName>
getColumnsForNewDataPart(
    MergeTreeData::DataPartPtr source_part,
    const Block & updated_header,
    NamesAndTypesList storage_columns,
    const SerializationInfoByName & serialization_infos,
    const MutationCommands & commands_for_interpreter,
    const MutationCommands & commands_for_removes)
{
    MutationCommands all_commands;
    all_commands.insert(all_commands.end(), commands_for_interpreter.begin(), commands_for_interpreter.end());
    all_commands.insert(all_commands.end(), commands_for_removes.begin(), commands_for_removes.end());

    NameSet removed_columns;
    NameToNameMap renamed_columns_to_from;
    NameToNameMap renamed_columns_from_to;
    ColumnsDescription part_columns(source_part->getColumns());
    NamesAndTypesList system_columns;

    bool supports_lightweight_deletes = source_part->supportLightweightDeleteMutate();

    bool deleted_mask_updated = false;
    bool has_delete_command = false;

    NameSet storage_columns_set;
    for (const auto & [name, _] : storage_columns)
        storage_columns_set.insert(name);

    for (const auto & command : all_commands)
    {
        if (command.type == MutationCommand::UPDATE)
        {
            for (const auto & [column_name, _] : command.column_to_update_expression)
            {
                if (column_name == RowExistsColumn::name
                    && supports_lightweight_deletes
                    && !storage_columns_set.contains(RowExistsColumn::name))
                    deleted_mask_updated = true;
            }
        }

        if (command.type == MutationCommand::DELETE || command.type == MutationCommand::APPLY_DELETED_MASK)
            has_delete_command = true;

        if (command.type == MutationCommand::LIGHTWEIGHT_DELETE)
            deleted_mask_updated = true;

        /// If we don't have this column in source part, than we don't need to materialize it
        if (!part_columns.has(command.column_name))
            continue;

        if (command.type == MutationCommand::DROP_COLUMN)
            removed_columns.insert(command.column_name);

        if (command.type == MutationCommand::RENAME_COLUMN)
        {
            renamed_columns_to_from.emplace(command.rename_to, command.column_name);
            renamed_columns_from_to.emplace(command.column_name, command.rename_to);
        }
    }

    auto persistent_virtuals = source_part->storage.getVirtualsPtr()->getNamesAndTypesList(VirtualsKind::Persistent);

    for (const auto & [name, type] : persistent_virtuals)
    {
        if (storage_columns_set.contains(name))
            continue;

        bool need_column = false;
        if (name == RowExistsColumn::name)
            need_column = deleted_mask_updated || (part_columns.has(name) && !has_delete_command);
        else
            need_column = part_columns.has(name);

        if (need_column)
        {
            storage_columns.emplace_back(name, type);
            storage_columns_set.insert(name);
        }
    }

    SerializationInfoByName new_serialization_infos;
    for (const auto & [name, old_info] : serialization_infos)
    {
        auto it = renamed_columns_from_to.find(name);
        auto new_name = it == renamed_columns_from_to.end() ? name : it->second;

        /// Column can be removed only in this data part by CLEAR COLUMN query.
        if (!storage_columns_set.contains(new_name) || removed_columns.contains(new_name))
            continue;

        /// In compact part we read all columns and all of them are in @updated_header.
        /// But in wide part we must keep serialization infos for columns that are not touched by mutation.
        if (!updated_header.has(new_name))
        {
            if (isWidePart(source_part))
                new_serialization_infos.emplace(new_name, old_info);
            continue;
        }

        auto old_type = part_columns.getPhysical(name).type;
        auto new_type = updated_header.getByName(new_name).type;

        SerializationInfo::Settings settings
        {
            .ratio_of_defaults_for_sparse = source_part->storage.getSettings()->ratio_of_defaults_for_sparse_serialization,
            .choose_kind = false
        };

        if (!new_type->supportsSparseSerialization() || settings.isAlwaysDefault())
            continue;

        auto new_info = new_type->createSerializationInfo(settings);
        if (!old_info->structureEquals(*new_info))
        {
            new_serialization_infos.emplace(new_name, std::move(new_info));
            continue;
        }

        new_info = old_info->createWithType(*old_type, *new_type, settings);
        new_serialization_infos.emplace(new_name, std::move(new_info));
    }

    /// In compact parts we read all columns, because they all stored in a single file
    if (!isWidePart(source_part) || !isFullPartStorage(source_part->getDataPartStorage()))
        return {updated_header.getNamesAndTypesList(), new_serialization_infos};

    const auto & source_columns = source_part->getColumns();
    std::unordered_map<String, DataTypePtr> source_columns_name_to_type;
    for (const auto & it : source_columns)
        source_columns_name_to_type[it.name] = it.type;

    for (auto it = storage_columns.begin(); it != storage_columns.end();)
    {
        if (updated_header.has(it->name))
        {
            auto updated_type = updated_header.getByName(it->name).type;
            if (updated_type != it->type)
                it->type = updated_type;
            ++it;
        }
        else
        {
            auto source_col = source_columns_name_to_type.find(it->name);
            if (source_col == source_columns_name_to_type.end())
            {
                /// Source part doesn't have column but some other column
                /// was renamed to it's name.
                auto renamed_it = renamed_columns_to_from.find(it->name);
                if (renamed_it != renamed_columns_to_from.end())
                {
                    source_col = source_columns_name_to_type.find(renamed_it->second);
                    if (source_col == source_columns_name_to_type.end())
                        it = storage_columns.erase(it);
                    else
                    {
                        /// Take a type from source part column.
                        /// It may differ from column type in storage.
                        it->type = source_col->second;
                        ++it;
                    }
                }
                else
                    it = storage_columns.erase(it);
            }
            else
            {
                /// Check that this column was renamed to some other name
                bool was_renamed = renamed_columns_from_to.contains(it->name);
                bool was_removed = removed_columns.contains(it->name);

                /// If we want to rename this column to some other name, than it
                /// should it's previous version should be dropped or removed
                if (renamed_columns_to_from.contains(it->name) && !was_renamed && !was_removed)
                    throw Exception(
                        ErrorCodes::LOGICAL_ERROR,
                        "Incorrect mutation commands, trying to rename column {} to {}, "
                        "but part {} already has column {}",
                        renamed_columns_to_from[it->name], it->name, source_part->name, it->name);

                /// Column was renamed and no other column renamed to it's name
                /// or column is dropped.
                if (!renamed_columns_to_from.contains(it->name) && (was_renamed || was_removed))
                {
                    it = storage_columns.erase(it);
                }
                else
                {

                    if (was_removed)
                    { /// DROP COLUMN xxx, RENAME COLUMN yyy TO xxx
                        auto renamed_from = renamed_columns_to_from.at(it->name);
                        auto maybe_name_and_type = source_columns.tryGetByName(renamed_from);
                        if (!maybe_name_and_type)
                            throw Exception(
                                ErrorCodes::LOGICAL_ERROR,
                                "Got incorrect mutation commands, column {} was renamed from {}, but it doesn't exist in source columns {}",
                                it->name, renamed_from, source_columns.toString());

                        it->type = maybe_name_and_type->type;
                    }
                    else
                    {
                        /// Take a type from source part column.
                        /// It may differ from column type in storage.
                        it->type = source_col->second;
                    }
                    ++it;
                }
            }
        }
    }

    return {storage_columns, new_serialization_infos};
}


static ExecuteTTLType shouldExecuteTTL(const StorageMetadataPtr & metadata_snapshot, const ColumnDependencies & dependencies)
{
    if (!metadata_snapshot->hasAnyTTL())
        return ExecuteTTLType::NONE;

    bool has_ttl_expression = false;

    for (const auto & dependency : dependencies)
    {
        if (dependency.kind == ColumnDependency::TTL_EXPRESSION)
            has_ttl_expression = true;

        if (dependency.kind == ColumnDependency::TTL_TARGET)
            return ExecuteTTLType::NORMAL;
    }
    return has_ttl_expression ? ExecuteTTLType::RECALCULATE : ExecuteTTLType::NONE;
}

static std::set<ColumnStatisticsPtr> getStatisticsToRecalculate(const StorageMetadataPtr & metadata_snapshot, const NameSet & materialized_stats)
{
    const auto & stats_factory = MergeTreeStatisticsFactory::instance();
    std::set<ColumnStatisticsPtr> stats_to_recalc;
    const auto & columns = metadata_snapshot->getColumns();
    for (const auto & col_desc : columns)
    {
        if (!col_desc.statistics.empty() && materialized_stats.contains(col_desc.name))
        {
            stats_to_recalc.insert(stats_factory.get(col_desc.statistics));
        }
    }
    return stats_to_recalc;
}

static NameSet getVectorIndicesToRebuild(
    const MutationCommands & commands)
{
    /// get need rebuild vector index column set
    NameSet rebuild_vector_index_column;
    for (const auto & command : commands)
        if (command.type == MutationCommand::Type::MATERIALIZE_COLUMN)
            rebuild_vector_index_column.insert(command.column_name);

    return rebuild_vector_index_column;
}

static void updateLWDMaskColumn(Block & block, UInt64 current_row, std::vector<UInt64> & current_task_deleted_row_ids)
{
    /// Quick return if no more deleted row ids
    if (current_task_deleted_row_ids.empty())
        return;

    /// Update _row_existing column based on sorted current_task_deleted_row_ids
    size_t begin = current_row;
    size_t end = current_row + block.rows();

    /// Current block [begin, end) doesn't contain deleted row ids
    if (begin > current_task_deleted_row_ids.back() || end <= current_task_deleted_row_ids.front())
        return;

    auto column = block.getByName(RowExistsColumn::name).column;
    ColumnUInt8 * row_exists_col = typeid_cast<ColumnUInt8 *>(column->assumeMutable().get());
    ColumnUInt8::Container & vec_in = row_exists_col->getData();

    /// Remove id from vector when updated
    for (auto it = current_task_deleted_row_ids.begin(); it != current_task_deleted_row_ids.end(); )
    {
        /// index of vec_in is [begin, end-1]
        if (*it >= end)
            break;

        if (*it >= begin)
        {
            /// Update _row_exists to 0
            size_t index_in_block = *it - begin;
            vec_in[index_in_block] = 0;

            it = current_task_deleted_row_ids.erase(it);
        }
        else /// should not happen
        {
            LOG_DEBUG(&Poco::Logger::get("updateLWDMaskColumn"), "Found deleted id {} smaller than begin offset of read block {}",
                        *it, begin);
            ++it;
        }
    }
}

/// Return set of indices which should be recalculated during mutation also
/// wraps input stream into additional expression stream
static std::set<MergeTreeIndexPtr> getIndicesToRecalculate(
    const MergeTreeDataPartPtr & source_part,
    QueryPipelineBuilder & builder,
    const StorageMetadataPtr & metadata_snapshot,
    ContextPtr context,
    const NameSet & materialized_indices)
{
    /// Checks if columns used in skipping indexes modified.
    const auto & index_factory = MergeTreeIndexFactory::instance();
    std::set<MergeTreeIndexPtr> indices_to_recalc;
    ASTPtr indices_recalc_expr_list = std::make_shared<ASTExpressionList>();
    const auto & indices = metadata_snapshot->getSecondaryIndices();
    bool is_full_part_storage = isFullPartStorage(source_part->getDataPartStorage());

    for (const auto & index : indices)
    {
        bool need_recalculate =
            materialized_indices.contains(index.name)
            || (!is_full_part_storage && source_part->hasSecondaryIndex(index.name));

        if (need_recalculate)
        {
            if (indices_to_recalc.insert(index_factory.get(index)).second)
            {
                ASTPtr expr_list = index.expression_list_ast->clone();
                for (const auto & expr : expr_list->children)
                    indices_recalc_expr_list->children.push_back(expr->clone());
            }
        }
    }

    if (!indices_to_recalc.empty() && builder.initialized())
    {
        auto indices_recalc_syntax = TreeRewriter(context).analyze(indices_recalc_expr_list, builder.getHeader().getNamesAndTypesList());
        auto indices_recalc_expr = ExpressionAnalyzer(
                indices_recalc_expr_list,
                indices_recalc_syntax, context).getActions(false);

        /// We can update only one column, but some skip idx expression may depend on several
        /// columns (c1 + c2 * c3). It works because this stream was created with help of
        /// MutationsInterpreter which knows about skip indices and stream 'in' already has
        /// all required columns.
        /// TODO move this logic to single place.
        builder.addTransform(std::make_shared<ExpressionTransform>(builder.getHeader(), indices_recalc_expr));
        builder.addTransform(std::make_shared<MaterializingTransform>(builder.getHeader()));
    }
    return indices_to_recalc;
}

static std::set<ProjectionDescriptionRawPtr> getProjectionsToRecalculate(
    const MergeTreeDataPartPtr & source_part,
    const StorageMetadataPtr & metadata_snapshot,
    const NameSet & materialized_projections)
{
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    bool is_full_part_storage = isFullPartStorage(source_part->getDataPartStorage());

    for (const auto & projection : metadata_snapshot->getProjections())
    {
        bool need_recalculate =
            materialized_projections.contains(projection.name)
            || (!is_full_part_storage
                && source_part->hasProjection(projection.name)
                && !source_part->hasBrokenProjection(projection.name));

        if (need_recalculate)
            projections_to_recalc.insert(&projection);
    }

    return projections_to_recalc;
}

static std::unordered_map<String, size_t> getStreamCounts(
    const MergeTreeDataPartPtr & data_part,
    const MergeTreeDataPartChecksums & source_part_checksums,
    const Names & column_names)
{
    std::unordered_map<String, size_t> stream_counts;

    for (const auto & column_name : column_names)
    {
        if (auto serialization = data_part->tryGetSerialization(column_name))
        {
            auto callback = [&](const ISerialization::SubstreamPath & substream_path)
            {
                auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(column_name, substream_path, source_part_checksums);
                if (stream_name)
                    ++stream_counts[*stream_name];
            };

            serialization->enumerateStreams(callback);
        }
    }

    return stream_counts;
}

/// Files, that we don't need to remove and don't need to hardlink, for example columns.txt and checksums.txt.
/// Because we will generate new versions of them after we perform mutation.
static NameSet collectFilesToSkip(
    const MergeTreeDataPartPtr & source_part,
    const MergeTreeDataPartPtr & new_part,
    const Block & updated_header,
    const std::set<MergeTreeIndexPtr> & indices_to_recalc,
    const String & mrk_extension,
    const std::set<ProjectionDescriptionRawPtr> & projections_to_recalc,
    const std::set<ColumnStatisticsPtr> & stats_to_recalc)
{
    /// Don't skip to create hard links for vector index files in mutations.
    NameSet files_to_skip = source_part->getFileNamesWithoutChecksums(false);

    /// Do not hardlink this file because it's always rewritten at the end of mutation.
    files_to_skip.insert(IMergeTreeDataPart::SERIALIZATION_FILE_NAME);

    for (const auto & index : indices_to_recalc)
    {
        /// Since MinMax index has .idx2 extension, we need to add correct extension.
        files_to_skip.insert(index->getFileName() + index->getSerializedFileExtension());
        files_to_skip.insert(index->getFileName() + mrk_extension);

        // Skip all full-text index files, for they will be rebuilt
        if (dynamic_cast<const MergeTreeIndexFullText *>(index.get()))
        {
            auto index_filename = index->getFileName();
            files_to_skip.insert(index_filename + ".gin_dict");
            files_to_skip.insert(index_filename + ".gin_post");
            files_to_skip.insert(index_filename + ".gin_sed");
            files_to_skip.insert(index_filename + ".gin_sid");
        }
    }

    for (const auto & projection : projections_to_recalc)
        files_to_skip.insert(projection->getDirectoryName());

    for (const auto & stat : stats_to_recalc)
        files_to_skip.insert(stat->getFileName() + STATS_FILE_SUFFIX);

    if (isWidePart(source_part))
    {
        auto new_stream_counts = getStreamCounts(new_part, source_part->checksums, new_part->getColumns().getNames());
        auto source_updated_stream_counts = getStreamCounts(source_part, source_part->checksums, updated_header.getNames());
        auto new_updated_stream_counts = getStreamCounts(new_part, source_part->checksums, updated_header.getNames());


        /// Skip all modified files in new part.
        for (const auto & [stream_name, _] : new_updated_stream_counts)
        {
            files_to_skip.insert(stream_name + ".bin");
            files_to_skip.insert(stream_name + mrk_extension);
        }

        /// Skip files that we read from source part and do not write in new part.
        /// E.g. ALTER MODIFY from LowCardinality(String) to String.
        for (const auto & [stream_name, _] : source_updated_stream_counts)
        {
            /// If we read shared stream and do not write it
            /// (e.g. while ALTER MODIFY COLUMN from array of Nested type to String),
            /// we need to hardlink its files, because they will be lost otherwise.
            bool need_hardlink = new_updated_stream_counts[stream_name] == 0 && new_stream_counts[stream_name] != 0;

            if (!need_hardlink)
            {
                files_to_skip.insert(stream_name + ".bin");
                files_to_skip.insert(stream_name + mrk_extension);
            }
        }
    }

    return files_to_skip;
}

#if USE_TANTIVY_SEARCH
static void removeTantivyIndexCache(MergeTreeData::DataPartPtr source_part, const MutationCommands & commands_for_removes)
{
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::Type::DROP_INDEX)
        {
            String skp_idx_name = INDEX_FILE_PREFIX + command.column_name;
            String source_data_part_relative_path = source_part->getDataPartStoragePtr()->getRelativePath();
            LOG_INFO(
                getLogger("removeTantivyIndexCache"),
                "INDEX_FILE_PREFIX: {}, command.column_name: {}, part relative_path: {}",
                INDEX_FILE_PREFIX,
                command.column_name,
                source_part->getDataPartStoragePtr()->getRelativePath());
            TantivyIndexStoreFactory::instance().dropIndex(skp_idx_name, source_part->getDataPartStoragePtr());
        }
    }
}
#endif

/// Apply commands to source_part i.e. remove and rename some columns in
/// source_part and return set of files, that have to be removed or renamed
/// from filesystem and in-memory checksums. Ordered result is important,
/// because we can apply renames that affects each other: x -> z, y -> x.
static NameToNameVector collectFilesForRenames(
    MergeTreeData::DataPartPtr source_part,
    MergeTreeData::DataPartPtr new_part,
    const MutationCommands & commands_for_removes,
    const String & mrk_extension)
{
    /// Collect counts for shared streams of different columns. As an example, Nested columns have shared stream with array sizes.
    auto stream_counts = getStreamCounts(source_part, source_part->checksums, source_part->getColumns().getNames());
    NameToNameVector rename_vector;
    NameSet collected_names;

    auto add_rename = [&rename_vector, &collected_names] (const std::string & file_rename_from, const std::string & file_rename_to)
    {
        if (collected_names.emplace(file_rename_from).second)
            rename_vector.emplace_back(file_rename_from, file_rename_to);
    };

    /// Remove old data
    for (const auto & command : commands_for_removes)
    {
        if (command.type == MutationCommand::Type::DROP_INDEX)
        {
            static const std::array<String, 2> suffixes = {".idx2", ".idx"};
            static const std::array<String, 4> gin_suffixes = {".gin_dict", ".gin_post", ".gin_seg", ".gin_sid"}; /// .gin_* means generalized inverted index (aka. full-text-index)
#if USE_TANTIVY_SEARCH
            static const std::array<String, 2> tantivy_suffixes
                = {TANTIVY_INDEX_OFFSET_FILE_TYPE, TANTIVY_INDEX_DATA_FILE_TYPE}; // tantivy index files
#endif

            for (const auto & suffix : suffixes)
            {
                const String filename = INDEX_FILE_PREFIX + command.column_name + suffix;
                const String filename_mrk = INDEX_FILE_PREFIX + command.column_name + mrk_extension;

                if (source_part->checksums.has(filename))
                {
                    add_rename(filename, "");
                    add_rename(filename_mrk, "");
                }
            }
            for (const auto & gin_suffix : gin_suffixes)
            {
                const String filename = INDEX_FILE_PREFIX + command.column_name + gin_suffix;
                if (source_part->checksums.has(filename))
                    add_rename(filename, "");
            }
#if USE_TANTIVY_SEARCH
            for (const auto & tantivy_suffix : tantivy_suffixes)
            {
                const String filename = INDEX_FILE_PREFIX + command.column_name + tantivy_suffix;
                if (source_part->checksums.has(filename))
                    add_rename(filename, "");
            }
#endif
        }
        else if (command.type == MutationCommand::Type::DROP_PROJECTION)
        {
            if (source_part->checksums.has(command.column_name + ".proj"))
                add_rename(command.column_name + ".proj", "");
        }
        else if (command.type == MutationCommand::Type::DROP_STATISTICS)
        {
            for (const auto & statistics_column_name : command.statistics_columns)
                if (source_part->checksums.has(STATS_FILE_PREFIX + statistics_column_name + STATS_FILE_SUFFIX))
                    add_rename(STATS_FILE_PREFIX + statistics_column_name + STATS_FILE_SUFFIX, "");
        }
        else if (isWidePart(source_part))
        {
            if (command.type == MutationCommand::Type::DROP_COLUMN)
            {
                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    auto stream_name = IMergeTreeDataPart::getStreamNameForColumn(command.column_name, substream_path, source_part->checksums);

                    /// Delete files if they are no longer shared with another column.
                    if (stream_name && --stream_counts[*stream_name] == 0)
                    {
                        add_rename(*stream_name + ".bin", "");
                        add_rename(*stream_name + mrk_extension, "");
                    }
                };

                if (auto serialization = source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);

                /// if we drop a column with statistics, we should also drop the stat file.
                if (source_part->checksums.has(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX))
                    add_rename(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX, "");
            }
            else if (command.type == MutationCommand::Type::RENAME_COLUMN)
            {
                String escaped_name_from = escapeForFileName(command.column_name);
                String escaped_name_to = escapeForFileName(command.rename_to);

                ISerialization::StreamCallback callback = [&](const ISerialization::SubstreamPath & substream_path)
                {
                    String full_stream_from = ISerialization::getFileNameForStream(command.column_name, substream_path);
                    String full_stream_to = boost::replace_first_copy(full_stream_from, escaped_name_from, escaped_name_to);

                    auto stream_from = IMergeTreeDataPart::getStreamNameOrHash(full_stream_from, source_part->checksums);
                    if (!stream_from)
                        return;

                    String stream_to;
                    auto storage_settings = source_part->storage.getSettings();

                    if (storage_settings->replace_long_file_name_to_hash && full_stream_to.size() > storage_settings->max_file_name_length)
                        stream_to = sipHash128String(full_stream_to);
                    else
                        stream_to = full_stream_to;

                    if (stream_from != stream_to)
                    {
                        add_rename(*stream_from + ".bin", stream_to + ".bin");
                        add_rename(*stream_from + mrk_extension, stream_to + mrk_extension);
                    }
                };

                if (auto serialization = source_part->tryGetSerialization(command.column_name))
                    serialization->enumerateStreams(callback);

                /// if we rename a column with statistics, we should also rename the stat file.
                if (source_part->checksums.has(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX))
                    add_rename(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX, STATS_FILE_PREFIX + command.rename_to + STATS_FILE_SUFFIX);
            }
            else if (command.type == MutationCommand::Type::READ_COLUMN)
            {
                /// Remove files for streams that exist in source_part,
                /// but were removed in new_part by MODIFY COLUMN from
                /// type with higher number of streams (e.g. LowCardinality -> String).

                auto old_streams = getStreamCounts(source_part, source_part->checksums, source_part->getColumns().getNames());
                auto new_streams = getStreamCounts(new_part, source_part->checksums, source_part->getColumns().getNames());

                for (const auto & [old_stream, _] : old_streams)
                {
                    if (!new_streams.contains(old_stream) && --stream_counts[old_stream] == 0)
                    {
                        add_rename(old_stream + ".bin", "");
                        add_rename(old_stream + mrk_extension, "");
                    }
                }
            }
        }
    }

    if (!source_part->getSerializationInfos().empty()
        && new_part->getSerializationInfos().empty())
    {
        rename_vector.emplace_back(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, "");
    }

    return rename_vector;
}


/// Initialize and write to disk new part fields like checksums, columns, etc.
void finalizeMutatedPart(
    const MergeTreeDataPartPtr & source_part,
    MergeTreeData::MutableDataPartPtr new_data_part,
    ExecuteTTLType execute_ttl_type,
    const CompressionCodecPtr & codec,
    ContextPtr context,
    StorageMetadataPtr metadata_snapshot,
    bool sync,
    const NameSet & rebuild_index_column)
{
    std::vector<std::unique_ptr<WriteBufferFromFileBase>> written_files;

    if (new_data_part->uuid != UUIDHelpers::Nil)
    {
        auto out = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::UUID_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out);
        writeUUIDText(new_data_part->uuid, out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::UUID_FILE_NAME].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out));
    }

    if (execute_ttl_type != ExecuteTTLType::NONE)
    {
        /// Write a file with ttl infos in json format.
        auto out_ttl = new_data_part->getDataPartStorage().writeFile("ttl.txt", 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_ttl);
        new_data_part->ttl_infos.write(out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files["ttl.txt"].file_size = out_hashing.count();
        new_data_part->checksums.files["ttl.txt"].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out_ttl));
    }

    if (!new_data_part->getSerializationInfos().empty())
    {
        auto out_serialization = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::SERIALIZATION_FILE_NAME, 4096, context->getWriteSettings());
        HashingWriteBuffer out_hashing(*out_serialization);
        new_data_part->getSerializationInfos().writeJSON(out_hashing);
        out_hashing.finalize();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_size = out_hashing.count();
        new_data_part->checksums.files[IMergeTreeDataPart::SERIALIZATION_FILE_NAME].file_hash = out_hashing.getHash();
        written_files.push_back(std::move(out_serialization));
    }

    {
        /// Write file with checksums.
        auto out_checksums = new_data_part->getDataPartStorage().writeFile("checksums.txt", 4096, context->getWriteSettings());
        new_data_part->checksums.write(*out_checksums);
        written_files.push_back(std::move(out_checksums));
    }

    {
        auto out_comp = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::DEFAULT_COMPRESSION_CODEC_FILE_NAME, 4096, context->getWriteSettings());
        DB::writeText(queryToString(codec->getFullCodecDesc()), *out_comp);
        written_files.push_back(std::move(out_comp));
    }

    {
        auto out_metadata = new_data_part->getDataPartStorage().writeFile(IMergeTreeDataPart::METADATA_VERSION_FILE_NAME, 4096, context->getWriteSettings());
        DB::writeText(metadata_snapshot->getMetadataVersion(), *out_metadata);
        written_files.push_back(std::move(out_metadata));
    }

    {
        /// Write a file with a description of columns.
        auto out_columns = new_data_part->getDataPartStorage().writeFile("columns.txt", 4096, context->getWriteSettings());
        new_data_part->getColumns().writeText(*out_columns);
        written_files.push_back(std::move(out_columns));
    }

    for (auto & file : written_files)
    {
        file->finalize();
        if (sync)
            file->sync();
    }
    /// Close files
    written_files.clear();

    new_data_part->rows_count = source_part->rows_count;
    new_data_part->index_granularity = source_part->index_granularity;
    new_data_part->setIndex(*source_part->getIndex());
    new_data_part->minmax_idx = source_part->minmax_idx;
    new_data_part->modification_time = time(nullptr);

    new_data_part->segments_mgr = source_part->segments_mgr->mutation(new_data_part, rebuild_index_column);

    /// Load rest projections which are hardlinked
    bool noop;
    new_data_part->loadProjections(false, false, noop, true /* if_not_loaded */);

    /// All information about sizes is stored in checksums.
    /// It doesn't make sense to touch filesystem for sizes.
    new_data_part->setBytesOnDisk(new_data_part->checksums.getTotalSizeOnDisk());
    new_data_part->setBytesUncompressedOnDisk(new_data_part->checksums.getTotalSizeUncompressedOnDisk());
    /// Also use information from checksums
    new_data_part->calculateColumnsAndSecondaryIndicesSizesOnDisk();

    new_data_part->default_codec = codec;

    /// TODO: Should new part inherit build error from old part?
    /// Retry build vector index for new parts.

#if USE_TANTIVY_SEARCH
    auto metadata = source_part->storage.getInMemoryMetadataPtr();
    if (metadata->hasSecondaryIndices() && metadata->getSecondaryIndices().hasFTS())
    {
        TantivyIndexStoreFactory::instance().mutate(
            source_part->getDataPartStoragePtr()->getRelativePath(), new_data_part->getDataPartStoragePtr()->getRelativePath());
    }
#endif
}

}

struct MutationContext
{
    MergeTreeData * data;
    MergeTreeDataMergerMutator * mutator;
    ActionBlocker * merges_blocker;
    TableLockHolder * holder;
    MergeListEntry * mutate_entry;

    LoggerPtr log{getLogger("MutateTask")};

    FutureMergedMutatedPartPtr future_part;
    MergeTreeData::DataPartPtr source_part;
    StorageMetadataPtr metadata_snapshot;

    MutationCommandsConstPtr commands;
    time_t time_of_mutation;
    ContextPtr context;
    ReservationSharedPtr space_reservation;

    CompressionCodecPtr compression_codec;

    std::unique_ptr<CurrentMetrics::Increment> num_mutations;

    QueryPipelineBuilder mutating_pipeline_builder;
    QueryPipeline mutating_pipeline; // in
    std::unique_ptr<PullingPipelineExecutor> mutating_executor;
    ProgressCallback progress_callback;
    Block updated_header;

    std::unique_ptr<MutationsInterpreter> interpreter;
    UInt64 watch_prev_elapsed = 0;
    std::unique_ptr<MergeStageProgress> stage_progress;

    MutationCommands commands_for_part;
    MutationCommands for_interpreter;
    MutationCommands for_file_renames;

    NamesAndTypesList storage_columns;
    NameSet materialized_indices;
    NameSet materialized_projections;
    NameSet materialized_statistics;

    MergeTreeData::MutableDataPartPtr new_data_part;
    IMergedBlockOutputStreamPtr out;

    String mrk_extension;

    std::vector<ProjectionDescriptionRawPtr> projections_to_build;
    IMergeTreeDataPart::MinMaxIndexPtr minmax_idx;

    std::set<MergeTreeIndexPtr> indices_to_recalc;
    std::set<ColumnStatisticsPtr> stats_to_recalc;
    std::set<ProjectionDescriptionRawPtr> projections_to_recalc;
    MergeTreeData::DataPart::Checksums existing_indices_stats_checksums;
    NameSet files_to_skip;
    NameToNameVector files_to_rename;

    bool need_sync;
    ExecuteTTLType execute_ttl_type{ExecuteTTLType::NONE};

    MergeTreeTransactionPtr txn;

    HardlinkedFiles hardlinked_files;

    bool need_prefix = true;

    scope_guard temporary_directory_lock;
    RWLockImpl::LockHolder move_index_read_lock;
    bool need_delete_rows{false};

    /// Whether we need to count lightweight delete rows in this mutation
    bool count_lightweight_deleted_rows;
    UInt64 execute_elapsed_ns = 0;

    /// Mark for optimized LWD
    bool is_optimized_lwd = false;
    /// deleted row ids for current task, used for optimized lightweight delete
    std::vector<UInt64> current_task_deleted_row_ids;

    /// need rebuild vector index
    NameSet rebuild_vector_index_column;
    /// need hardlink vector index files
    NameSet hardlink_vector_index_files;
};

using MutationContextPtr = std::shared_ptr<MutationContext>;


class MergeProjectionPartsTask : public IExecutableTask
{
public:

    MergeProjectionPartsTask(
        String name_,
        MergeTreeData::MutableDataPartsVector && parts_,
        const ProjectionDescription & projection_,
        size_t & block_num_,
        MutationContextPtr ctx_)
        : name(std::move(name_))
        , parts(std::move(parts_))
        , projection(projection_)
        , block_num(block_num_)
        , ctx(ctx_)
        , log(getLogger("MergeProjectionPartsTask"))
        {
            LOG_DEBUG(log, "Selected {} projection_parts from {} to {}", parts.size(), parts.front()->name, parts.back()->name);
            level_parts[current_level] = std::move(parts);
        }

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        auto & current_level_parts = level_parts[current_level];
        auto & next_level_parts = level_parts[next_level];

        MergeTreeData::MutableDataPartsVector selected_parts;
        while (selected_parts.size() < max_parts_to_merge_in_one_level && !current_level_parts.empty())
        {
            selected_parts.push_back(std::move(current_level_parts.back()));
            current_level_parts.pop_back();
        }

        if (selected_parts.empty())
        {
            if (next_level_parts.empty())
            {
                LOG_WARNING(log, "There is no projection parts merged");

                /// Task is finished
                return false;
            }
            current_level = next_level;
            ++next_level;
        }
        else if (selected_parts.size() == 1)
        {
            if (next_level_parts.empty())
            {
                LOG_DEBUG(log, "Merged a projection part in level {}", current_level);
                selected_parts[0]->renameTo(projection.name + ".proj", true);
                selected_parts[0]->setName(projection.name);
                selected_parts[0]->is_temp = false;
                ctx->new_data_part->addProjectionPart(name, std::move(selected_parts[0]));

                /// Task is finished
                return false;
            }
            else
            {
                LOG_DEBUG(log, "Forwarded part {} in level {} to next level", selected_parts[0]->name, current_level);
                next_level_parts.push_back(std::move(selected_parts[0]));
            }
        }
        else if (selected_parts.size() > 1)
        {
            // Generate a unique part name
            ++block_num;
            auto projection_future_part = std::make_shared<FutureMergedMutatedPart>();
            MergeTreeData::DataPartsVector const_selected_parts(
                std::make_move_iterator(selected_parts.begin()), std::make_move_iterator(selected_parts.end()));
            projection_future_part->assign(std::move(const_selected_parts));
            projection_future_part->name = fmt::format("{}_{}", projection.name, ++block_num);
            projection_future_part->part_info = {"all", 0, 0, 0};

            MergeTreeData::MergingParams projection_merging_params;
            projection_merging_params.mode = MergeTreeData::MergingParams::Ordinary;
            if (projection.type == ProjectionDescription::Type::Aggregate)
                projection_merging_params.mode = MergeTreeData::MergingParams::Aggregating;

            LOG_DEBUG(log, "Merged {} parts in level {} to {}", selected_parts.size(), current_level, projection_future_part->name);
            auto tmp_part_merge_task = ctx->mutator->mergePartsToTemporaryPart(
                projection_future_part,
                projection.metadata,
                ctx->mutate_entry,
                std::make_unique<MergeListElement>((*ctx->mutate_entry)->table_id, projection_future_part, ctx->context),
                *ctx->holder,
                ctx->time_of_mutation,
                ctx->context,
                ctx->space_reservation,
                false, // TODO Do we need deduplicate for projections
                {},
                false, // no cleanup
                projection_merging_params,
                NO_TRANSACTION_PTR,
                /* need_prefix */ true,
                ctx->new_data_part.get(),
                ".tmp_proj");

            next_level_parts.push_back(executeHere(tmp_part_merge_task));
            next_level_parts.back()->is_temp = true;
        }

        /// Need execute again
        return true;
    }

private:
    String name;
    MergeTreeData::MutableDataPartsVector parts;
    const ProjectionDescription & projection;
    size_t & block_num;
    MutationContextPtr ctx;

    LoggerPtr log;

    std::map<size_t, MergeTreeData::MutableDataPartsVector> level_parts;
    size_t current_level = 0;
    size_t next_level = 1;

    /// TODO(nikitamikhaylov): make this constant a setting
    static constexpr size_t max_parts_to_merge_in_one_level = 10;
};


// This class is responsible for:
// 1. get projection pipeline and a sink to write parts
// 2. build an executor that can write block to the input stream (actually we can write through it to generate as many parts as possible)
// 3. finalize the pipeline so that all parts are merged into one part

// In short it executed a mutation for the part an original part and for its every projection

/**
 *
 * An overview of how the process of mutation works for projections:
 *
 * The mutation for original parts is executed block by block,
 * but additionally we execute a SELECT query for each projection over a current block.
 * And we store results to a map : ProjectionName -> ArrayOfParts.
 *
 * Then, we have to merge all parts for each projection. But we will have constraint:
 * We cannot execute merge on more than 10 parts simulatiously.
 * So we build a tree of merges. At the beginning all the parts have level 0.
 * At each step we take not bigger than 10 parts from the same level
 * and merge it into a bigger part with incremented level.
 */
class PartMergerWriter
{
public:

    explicit PartMergerWriter(MutationContextPtr ctx_)
        : ctx(ctx_), projections(ctx->metadata_snapshot->projections)
    {
    }

    bool execute()
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                if (ctx->is_optimized_lwd && !ctx->source_part->hasLightweightDelete())
                    state = State::NEED_OPTIMIZE_LWD_ON_PART;
                else
                    state = State::NEED_MUTATE_ORIGINAL_PART;
                return true;
            }
            case State::NEED_OPTIMIZE_LWD_ON_PART:
            {
                if (optimizedLWDMutate())
                   return true;

                state = State::NEED_MERGE_PROJECTION_PARTS;
                return true;
            }
            case State::NEED_MUTATE_ORIGINAL_PART:
            {
                if (mutateOriginalPartAndPrepareProjections())
                    return true;

                state = State::NEED_MERGE_PROJECTION_PARTS;
                return true;
            }
            case State::NEED_MERGE_PROJECTION_PARTS:
            {
                if (iterateThroughAllProjections())
                    return true;

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                finalize();
                return false;
            }
        }
        return false;
    }

private:
    void prepare();
    bool optimizedLWDMutate();
    bool mutateOriginalPartAndPrepareProjections();
    void writeTempProjectionPart(size_t projection_idx, Chunk chunk);
    void finalizeTempProjections();
    bool iterateThroughAllProjections();
    void constructTaskForProjectionPartsMerge();
    void finalize();

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_MUTATE_ORIGINAL_PART,
        NEED_MERGE_PROJECTION_PARTS,
        NEED_OPTIMIZE_LWD_ON_PART, /// Lightweight delete on part without LWD mask column

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;

    size_t block_num = 0;

    /// Used for row id when LWD needs to collect deleted row ids
    UInt64 part_offset = 0;

    using ProjectionNameToItsBlocks = std::map<String, MergeTreeData::MutableDataPartsVector>;
    ProjectionNameToItsBlocks projection_parts;
    std::move_iterator<ProjectionNameToItsBlocks::iterator> projection_parts_iterator;

    std::vector<Squashing> projection_squashes;
    const ProjectionsDescription & projections;

    ExecutableTaskPtr merge_projection_parts_task_ptr;

    /// Existing rows count calculated during part writing.
    /// It is initialized in prepare(), calculated in mutateOriginalPartAndPrepareProjections()
    /// and set to new_data_part in finalize()
    size_t existing_rows_count;
};


void PartMergerWriter::prepare()
{
    const auto & settings = ctx->context->getSettingsRef();

    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        // We split the materialization into multiple stages similar to the process of INSERT SELECT query.
        projection_squashes.emplace_back(ctx->updated_header, settings.min_insert_block_size_rows, settings.min_insert_block_size_bytes);
    }

    existing_rows_count = 0;
}


bool PartMergerWriter::optimizedLWDMutate()
{
    /// If source part has no lightweight delete, no need to call mutating_executor to read _row_exists mask column
    Block cur_block;
    size_t total_rows = ctx->source_part->rows_count;

    /// Update existing rows count and deleted row ids for new part
    if (part_offset == 0)
    {
        ctx->new_data_part->existing_rows_count = total_rows - ctx->current_task_deleted_row_ids.size();
        if (ctx->new_data_part->isDeletedMaskUpdated())
            ctx->new_data_part->deleted_row_ids = ctx->current_task_deleted_row_ids;
    }

    if (MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry) && part_offset < total_rows)
    {
        const auto & settings = ctx->context->getSettingsRef();
        size_t max_block_rows = settings.max_block_size;
        size_t remaining_rows = total_rows - part_offset;
        size_t rows_to_create = remaining_rows > max_block_rows ? max_block_rows : remaining_rows;

        ColumnPtr new_mask = ColumnUInt8::create(rows_to_create, 1);

        cur_block.insert(ColumnWithTypeAndName(new_mask, RowExistsColumn::type, RowExistsColumn::name));

        MutationHelpers::updateLWDMaskColumn(cur_block, part_offset, ctx->current_task_deleted_row_ids);

        ctx->out->write(cur_block);

        part_offset += rows_to_create;
        (*ctx->mutate_entry)->rows_written += cur_block.rows();
        (*ctx->mutate_entry)->bytes_written_uncompressed += cur_block.bytes();

        /// Need execute again
        return true;
    }

    /// Let's move on to the next stage
    return false;
}


bool PartMergerWriter::mutateOriginalPartAndPrepareProjections()
{
    Stopwatch watch(CLOCK_MONOTONIC_COARSE);
    UInt64 step_time_ms = ctx->data->getSettings()->background_task_preferred_step_execution_time_ms.totalMilliseconds();

    do
    {
        Block cur_block;
        Block projection_header;

        MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry);

        if (!ctx->mutating_executor->pull(cur_block))
        {
            finalizeTempProjections();
            return false;
        }

        /// Update mask column for optimized lightweight delete
        if (ctx->is_optimized_lwd)
            MutationHelpers::updateLWDMaskColumn(cur_block, part_offset, ctx->current_task_deleted_row_ids);

        if (ctx->minmax_idx)
            ctx->minmax_idx->update(cur_block, MergeTreeData::getMinMaxColumnsNames(ctx->metadata_snapshot->getPartitionKey()));

        ctx->out->write(cur_block);

        /// TODO: move this calculation to DELETE FROM mutation
        /// Try to collect deleted row ids when getting existing rows count for LWD
        /// count_lightweight_deleted_rows is false when setting exclude_deleted_rows_for_part_size_in_merge is false.
        if (ctx->count_lightweight_deleted_rows || ctx->new_data_part->isDeletedMaskUpdated())
            existing_rows_count += MutationHelpers::getExistingRowsCount(
                cur_block, ctx->new_data_part->isDeletedMaskUpdated(), part_offset, ctx->new_data_part->deleted_row_ids);

        for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
        {
            Chunk squashed_chunk;

            {
                ProfileEventTimeIncrement<Microseconds> projection_watch(ProfileEvents::MutateTaskProjectionsCalculationMicroseconds);
                Block block_to_squash = ctx->projections_to_build[i]->calculate(cur_block, ctx->context);

                projection_squashes[i].setHeader(block_to_squash.cloneEmpty());
                squashed_chunk = Squashing::squash(projection_squashes[i].add({block_to_squash.getColumns(), block_to_squash.rows()}));
            }

            if (squashed_chunk)
                writeTempProjectionPart(i, std::move(squashed_chunk));
        }

        (*ctx->mutate_entry)->rows_written += cur_block.rows();
        (*ctx->mutate_entry)->bytes_written_uncompressed += cur_block.bytes();
    } while (watch.elapsedMilliseconds() < step_time_ms);

    /// Need execute again
    return true;
}

void PartMergerWriter::writeTempProjectionPart(size_t projection_idx, Chunk chunk)
{
    const auto & projection = *ctx->projections_to_build[projection_idx];
    const auto & projection_plan = projection_squashes[projection_idx];

    auto result = projection_plan.getHeader().cloneWithColumns(chunk.detachColumns());

    auto tmp_part = MergeTreeDataWriter::writeTempProjectionPart(
        *ctx->data,
        ctx->log,
        result,
        projection,
        ctx->new_data_part.get(),
        ++block_num);

    tmp_part.finalize();
    tmp_part.part->getDataPartStorage().commitTransaction();
    projection_parts[projection.name].emplace_back(std::move(tmp_part.part));
}

void PartMergerWriter::finalizeTempProjections()
{
    // Write the last block
    for (size_t i = 0, size = ctx->projections_to_build.size(); i < size; ++i)
    {
        auto squashed_chunk = Squashing::squash(projection_squashes[i].flush());
        if (squashed_chunk)
            writeTempProjectionPart(i, std::move(squashed_chunk));
    }

    projection_parts_iterator = std::make_move_iterator(projection_parts.begin());

    /// Maybe there are no projections ?
    if (projection_parts_iterator != std::make_move_iterator(projection_parts.end()))
        constructTaskForProjectionPartsMerge();
}

void PartMergerWriter::constructTaskForProjectionPartsMerge()
{
    auto && [name, parts] = *projection_parts_iterator;
    const auto & projection = projections.get(name);

    merge_projection_parts_task_ptr = std::make_unique<MergeProjectionPartsTask>
    (
        name,
        std::move(parts),
        projection,
        block_num,
        ctx
    );
}


bool PartMergerWriter::iterateThroughAllProjections()
{
    /// In case if there are no projections we didn't construct a task
    if (!merge_projection_parts_task_ptr)
        return false;

    if (merge_projection_parts_task_ptr->executeStep())
        return true;

    ++projection_parts_iterator;

    if (projection_parts_iterator == std::make_move_iterator(projection_parts.end()))
        return false;

    constructTaskForProjectionPartsMerge();

    return true;
}

void PartMergerWriter::finalize()
{
    if (ctx->count_lightweight_deleted_rows)
        ctx->new_data_part->existing_rows_count = existing_rows_count;
}

class MutateAllPartColumnsTask : public IExecutableTask
{
public:

    explicit MutateAllPartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();

                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

private:

    void prepare()
    {
        if (ctx->new_data_part->isStoredOnDisk())
            ctx->new_data_part->getDataPartStorage().createDirectories();

        /// Note: this is done before creating input streams, because otherwise data.data_parts_mutex
        /// (which is locked in data.getTotalActiveSizeInBytes())
        /// (which is locked in shared mode when input streams are created) and when inserting new data
        /// the order is reverse. This annoys TSan even though one lock is locked in shared mode and thus
        /// deadlock is impossible.
        ctx->compression_codec
            = ctx->data->getCompressionCodecForPart(ctx->source_part->getBytesOnDisk(), ctx->source_part->ttl_infos, ctx->time_of_mutation);

        NameSet entries_to_hardlink;

        NameSet removed_indices;
        NameSet removed_stats;
        /// A stat file need to be renamed iff the column is renamed.
        NameToNameMap renamed_stats;

        for (const auto & command : ctx->for_file_renames)
        {
            if (command.type == MutationCommand::DROP_INDEX)
                removed_indices.insert(command.column_name);
            else if (command.type == MutationCommand::DROP_STATISTICS)
                for (const auto & column_name : command.statistics_columns)
                    removed_stats.insert(column_name);
            else if (command.type == MutationCommand::RENAME_COLUMN
                     && ctx->source_part->checksums.files.contains(STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX))
                renamed_stats[STATS_FILE_PREFIX + command.column_name + STATS_FILE_SUFFIX] = STATS_FILE_PREFIX + command.rename_to + STATS_FILE_SUFFIX;
        }

        bool is_full_part_storage = isFullPartStorage(ctx->new_data_part->getDataPartStorage());
        const auto & indices = ctx->metadata_snapshot->getSecondaryIndices();

        MergeTreeIndices skip_indices;
        for (const auto & idx : indices)
        {
            if (removed_indices.contains(idx.name))
                continue;

            bool need_recalculate =
                ctx->materialized_indices.contains(idx.name)
                || (!is_full_part_storage && ctx->source_part->hasSecondaryIndex(idx.name));

            if (need_recalculate)
            {
                skip_indices.push_back(MergeTreeIndexFactory::instance().get(idx));
            }
            else
            {
                auto prefix = fmt::format("{}{}.", INDEX_FILE_PREFIX, idx.name);
                auto it = ctx->source_part->checksums.files.upper_bound(prefix);
                while (it != ctx->source_part->checksums.files.end())
                {
                    if (!startsWith(it->first, prefix))
                        break;

                    entries_to_hardlink.insert(it->first);
                    ctx->existing_indices_stats_checksums.addFile(it->first, it->second.file_size, it->second.file_hash);
                    ++it;
                }
            }
        }

        ColumnsStatistics stats_to_rewrite;
        const auto & columns = ctx->metadata_snapshot->getColumns();
        for (const auto & col : columns)
        {
            if (col.statistics.empty() || removed_stats.contains(col.name))
                continue;

            if (ctx->materialized_statistics.contains(col.name))
            {
                stats_to_rewrite.push_back(MergeTreeStatisticsFactory::instance().get(col.statistics));
            }
            else
            {
                /// We do not hard-link statistics which
                /// 1. In `DROP STATISTICS` statement. It is filtered by `removed_stats`
                /// 2. Not in column list anymore, including `DROP COLUMN`. It is not touched by this loop.
                String stat_file_name = STATS_FILE_PREFIX + col.name + STATS_FILE_SUFFIX;
                auto it = ctx->source_part->checksums.files.find(stat_file_name);
                if (it != ctx->source_part->checksums.files.end())
                {
                    entries_to_hardlink.insert(it->first);
                    ctx->existing_indices_stats_checksums.addFile(it->first, it->second.file_size, it->second.file_hash);
                }
            }
        }

        NameSet removed_projections;
        for (const auto & command : ctx->for_file_renames)
        {
            if (command.type == MutationCommand::DROP_PROJECTION)
                removed_projections.insert(command.column_name);
        }

        const auto & projections = ctx->metadata_snapshot->getProjections();
        for (const auto & projection : projections)
        {
            if (removed_projections.contains(projection.name))
                continue;

            bool need_recalculate =
                ctx->materialized_projections.contains(projection.name)
                || (!is_full_part_storage
                    && ctx->source_part->hasProjection(projection.name)
                    && !ctx->source_part->hasBrokenProjection(projection.name));

            if (need_recalculate)
            {
                ctx->projections_to_build.push_back(&projection);
            }
            else
            {
                if (ctx->source_part->checksums.has(projection.getDirectoryName()))
                    entries_to_hardlink.insert(projection.getDirectoryName());
            }
        }

        NameSet hardlinked_files;
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            /// Skip files that are not in the list of files to hardlink
            if (endsWith(it->name(), ".vidx3") && !ctx->hardlink_vector_index_files.contains(it->name()))
                continue;

            if (!entries_to_hardlink.contains(it->name()))
            {
                if (renamed_stats.contains(it->name()))
                {
                    ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), renamed_stats.at(it->name()));
                    hardlinked_files.insert(it->name());
                    /// Also we need to "rename" checksums to finalize correctly.
                    const auto & check_sum = ctx->source_part->checksums.files.at(it->name());
                    ctx->existing_indices_stats_checksums.addFile(renamed_stats.at(it->name()), check_sum.file_size, check_sum.file_hash);
                }
            }
            else if (it->isFile())
            {
                ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                    ctx->source_part->getDataPartStorage(), it->name(), it->name());
                hardlinked_files.insert(it->name());
            }
            else
            {
                // it's a projection part directory
                ctx->new_data_part->getDataPartStorage().createProjection(it->name());

                auto projection_data_part_storage_src = ctx->source_part->getDataPartStorage().getProjection(it->name());
                auto projection_data_part_storage_dst = ctx->new_data_part->getDataPartStorage().getProjection(it->name());

                for (auto p_it = projection_data_part_storage_src->iterate(); p_it->isValid(); p_it->next())
                {
                    projection_data_part_storage_dst->createHardLinkFrom(
                        *projection_data_part_storage_src, p_it->name(), p_it->name());

                    auto file_name_with_projection_prefix = fs::path(projection_data_part_storage_src->getPartDirectory()) / p_it->name();
                    hardlinked_files.insert(file_name_with_projection_prefix);
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = std::move(hardlinked_files);

        if (!ctx->mutating_pipeline_builder.initialized())
            throw Exception(ErrorCodes::LOGICAL_ERROR, "Cannot mutate part columns with uninitialized mutations stream. It's a bug");

        auto builder = std::make_unique<QueryPipelineBuilder>(std::move(ctx->mutating_pipeline_builder));

        if (ctx->metadata_snapshot->hasPrimaryKey() || ctx->metadata_snapshot->hasSecondaryIndices())
        {
            builder->addTransform(std::make_shared<ExpressionTransform>(
                builder->getHeader(), ctx->data->getPrimaryKeyAndSkipIndicesExpression(ctx->metadata_snapshot, skip_indices)));

            builder->addTransform(std::make_shared<MaterializingTransform>(builder->getHeader()));
        }

        PreparedSets::Subqueries subqueries;

        if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
        {
            auto transform = std::make_shared<TTLTransform>(ctx->context, builder->getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
            subqueries = transform->getSubqueries();
            builder->addTransform(std::move(transform));
        }

        if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
        {
            auto transform = std::make_shared<TTLCalcTransform>(ctx->context, builder->getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
            subqueries = transform->getSubqueries();
            builder->addTransform(std::move(transform));
        }

        if (!subqueries.empty())
            builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), ctx->context);

        ctx->minmax_idx = std::make_shared<IMergeTreeDataPart::MinMaxIndex>();

        MergeTreeIndexGranularity computed_granularity;
        bool has_delete = false;

        for (auto & command_for_interpreter : ctx->for_interpreter)
        {
            if (command_for_interpreter.type == MutationCommand::DELETE
                || command_for_interpreter.type == MutationCommand::APPLY_DELETED_MASK)
            {
                has_delete = true;
                break;
            }
        }

        /// Reuse source part granularity if mutation does not change number of rows
        if (!has_delete && ctx->execute_ttl_type == ExecuteTTLType::NONE)
            computed_granularity = ctx->source_part->index_granularity;

        ctx->out = std::make_shared<MergedBlockOutputStream>(
            ctx->new_data_part,
            ctx->metadata_snapshot,
            ctx->new_data_part->getColumns(),
            skip_indices,
            stats_to_rewrite,
            ctx->compression_codec,
            ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID,
            /*reset_columns=*/ true,
            /*blocks_are_granules_size=*/ false,
            ctx->context->getWriteSettings(),
            computed_granularity);

        ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
        ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
        /// Is calculated inside MergeProgressCallback.
        ctx->mutating_pipeline.disableProfileEventUpdate();
        ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

        part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
    }


    void finalize()
    {
        bool noop;
        ctx->new_data_part->minmax_idx = std::move(ctx->minmax_idx);
        ctx->new_data_part->loadProjections(false, false, noop, true /* if_not_loaded */);
        ctx->mutating_executor.reset();
        ctx->mutating_pipeline.reset();

        static_pointer_cast<MergedBlockOutputStream>(ctx->out)->finalizePart(
            ctx->new_data_part, ctx->need_sync, nullptr, &ctx->existing_indices_stats_checksums);
        ctx->out.reset();

        /// Create hardlinks for vector index files in simple built part or decoupled part when MutateAllPartColumns
        /// Reuse vector index when no rows are deleted
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            String file_name = it->name();
            if (!endsWith(file_name, VECTOR_INDEX_FILE_SUFFIX))
                continue;

            ctx->new_data_part->getDataPartStorage().createHardLinkFrom(ctx->source_part->getDataPartStorage(), file_name, file_name);
        }

        ctx->new_data_part->segments_mgr = ctx->source_part->segments_mgr->mutation(ctx->new_data_part, ctx->rebuild_vector_index_column);
        /// TODO: build index marks the ector_indexed in some unsuccessful cases. If fixed, vector_files_found can be removed.
        /// TODO: Should new part inherit build error from old part?
        /// Retry build vector index for new parts.
    }

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};

    MutationContextPtr ctx;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task;
};


class MutateSomePartColumnsTask : public IExecutableTask
{
public:
    explicit MutateSomePartColumnsTask(MutationContextPtr ctx_) : ctx(ctx_) {}

    void onCompleted() override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    StorageID getStorageID() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    Priority getPriority() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }
    String getQueryId() const override { throw Exception(ErrorCodes::LOGICAL_ERROR, "Not implemented"); }

    bool executeStep() override
    {
        switch (state)
        {
            case State::NEED_PREPARE:
            {
                prepare();
                state = State::NEED_EXECUTE;
                return true;
            }
            case State::NEED_EXECUTE:
            {
                if (part_merger_writer_task && part_merger_writer_task->execute())
                    return true;

                state = State::NEED_FINALIZE;
                return true;
            }
            case State::NEED_FINALIZE:
            {
                finalize();

                state = State::SUCCESS;
                return true;
            }
            case State::SUCCESS:
            {
                return false;
            }
        }
        return false;
    }

private:

    void prepare()
    {
        if (ctx->execute_ttl_type != ExecuteTTLType::NONE)
            ctx->files_to_skip.insert("ttl.txt");

        ctx->new_data_part->getDataPartStorage().createDirectories();

        /// We should write version metadata on part creation to distinguish it from parts that were created without transaction.
        TransactionID tid = ctx->txn ? ctx->txn->tid : Tx::PrehistoricTID;
        /// NOTE do not pass context for writing to system.transactions_info_log,
        /// because part may have temporary name (with temporary block numbers). Will write it later.
        ctx->new_data_part->version.setCreationTID(tid, nullptr);
        ctx->new_data_part->storeVersionMetadata();

        auto settings = ctx->source_part->storage.getSettings();

        NameSet hardlinked_files;

        /// NOTE: Renames must be done in order
        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty()) /// It's DROP COLUMN
            {
                /// pass
            }
            else
            {
                ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                    ctx->source_part->getDataPartStorage(), rename_from, rename_to);
                hardlinked_files.insert(rename_from);
            }
        }
        /// Create hardlinks for unchanged files
        for (auto it = ctx->source_part->getDataPartStorage().iterate(); it->isValid(); it->next())
        {
            if (ctx->files_to_skip.contains(it->name()))
                continue;

            String file_name = it->name();

            auto rename_it = std::find_if(ctx->files_to_rename.begin(), ctx->files_to_rename.end(), [&file_name](const auto & rename_pair)
            {
                return rename_pair.first == file_name;
            });

            if (rename_it != ctx->files_to_rename.end())
            {
                /// RENAMEs and DROPs already processed
                continue;
            }

            /// Skip vector index files if they are not in the list of files to hardlink
            if (endsWith(it->name(), ".vidx3") && !ctx->hardlink_vector_index_files.contains(it->name()))
                continue;

            String destination = it->name();

            if (it->isFile())
            {
                if (settings->always_use_copy_instead_of_hardlinks)
                {
                    ctx->new_data_part->getDataPartStorage().copyFileFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), destination);
                }
                else
                {
                    ctx->new_data_part->getDataPartStorage().createHardLinkFrom(
                        ctx->source_part->getDataPartStorage(), it->name(), destination);

                    hardlinked_files.insert(it->name());
                }
            }
            else if (!endsWith(it->name(), ".tmp_proj")) // ignore projection tmp merge dir
            {
                // it's a projection part directory
                ctx->new_data_part->getDataPartStorage().createProjection(destination);

                auto projection_data_part_storage_src = ctx->source_part->getDataPartStorage().getProjection(destination);
                auto projection_data_part_storage_dst = ctx->new_data_part->getDataPartStorage().getProjection(destination);

                for (auto p_it = projection_data_part_storage_src->iterate(); p_it->isValid(); p_it->next())
                {
                    if (settings->always_use_copy_instead_of_hardlinks)
                    {
                        projection_data_part_storage_dst->copyFileFrom(
                            *projection_data_part_storage_src, p_it->name(), p_it->name());
                    }
                    else
                    {
                        auto file_name_with_projection_prefix = fs::path(projection_data_part_storage_src->getPartDirectory()) / p_it->name();

                        projection_data_part_storage_dst->createHardLinkFrom(
                            *projection_data_part_storage_src, p_it->name(), p_it->name());

                        hardlinked_files.insert(file_name_with_projection_prefix);
                    }
                }
            }
        }

        /// Tracking of hardlinked files required for zero-copy replication.
        /// We don't remove them when we delete last copy of source part because
        /// new part can use them.
        ctx->hardlinked_files.source_table_shared_id = ctx->source_part->storage.getTableSharedID();
        ctx->hardlinked_files.source_part_name = ctx->source_part->name;
        ctx->hardlinked_files.hardlinks_from_source_part = std::move(hardlinked_files);

        (*ctx->mutate_entry)->columns_written = ctx->storage_columns.size() - ctx->updated_header.columns();

        ctx->new_data_part->checksums = ctx->source_part->checksums;

        ctx->compression_codec = ctx->source_part->default_codec;

        if (ctx->mutating_pipeline_builder.initialized())
        {
            auto builder = std::make_unique<QueryPipelineBuilder>(std::move(ctx->mutating_pipeline_builder));
            PreparedSets::Subqueries subqueries;

            if (ctx->execute_ttl_type == ExecuteTTLType::NORMAL)
            {
                auto transform = std::make_shared<TTLTransform>(ctx->context, builder->getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
                subqueries = transform->getSubqueries();
                builder->addTransform(std::move(transform));
            }

            if (ctx->execute_ttl_type == ExecuteTTLType::RECALCULATE)
            {
                auto transform = std::make_shared<TTLCalcTransform>(ctx->context, builder->getHeader(), *ctx->data, ctx->metadata_snapshot, ctx->new_data_part, ctx->time_of_mutation, true);
                subqueries = transform->getSubqueries();
                builder->addTransform(std::move(transform));
            }

            if (!subqueries.empty())
                builder = addCreatingSetsTransform(std::move(builder), std::move(subqueries), ctx->context);

            ctx->out = std::make_shared<MergedColumnOnlyOutputStream>(
                ctx->new_data_part,
                ctx->metadata_snapshot,
                ctx->updated_header.getNamesAndTypesList(),
                ctx->compression_codec,
                std::vector<MergeTreeIndexPtr>(ctx->indices_to_recalc.begin(), ctx->indices_to_recalc.end()),
                ColumnsStatistics(ctx->stats_to_recalc.begin(), ctx->stats_to_recalc.end()),
                nullptr,
                ctx->source_part->index_granularity,
                &ctx->source_part->index_granularity_info
            );

            ctx->mutating_pipeline = QueryPipelineBuilder::getPipeline(std::move(*builder));
            ctx->mutating_pipeline.setProgressCallback(ctx->progress_callback);
            /// Is calculated inside MergeProgressCallback.
            ctx->mutating_pipeline.disableProfileEventUpdate();
            ctx->mutating_executor = std::make_unique<PullingPipelineExecutor>(ctx->mutating_pipeline);

            ctx->projections_to_build = std::vector<ProjectionDescriptionRawPtr>{ctx->projections_to_recalc.begin(), ctx->projections_to_recalc.end()};

            part_merger_writer_task = std::make_unique<PartMergerWriter>(ctx);
        }
    }


    void finalize()
    {
        if (ctx->mutating_executor)
        {
            ctx->mutating_executor.reset();
            ctx->mutating_pipeline.reset();

            auto changed_checksums =
                static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out)->fillChecksums(
                    ctx->new_data_part, ctx->new_data_part->checksums);
            ctx->new_data_part->checksums.add(std::move(changed_checksums));

            static_pointer_cast<MergedColumnOnlyOutputStream>(ctx->out)->finish(ctx->need_sync);
        }

        for (const auto & [rename_from, rename_to] : ctx->files_to_rename)
        {
            if (rename_to.empty() && ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
            else if (ctx->new_data_part->checksums.files.contains(rename_from))
            {
                ctx->new_data_part->checksums.files[rename_to] = ctx->new_data_part->checksums.files[rename_from];
                ctx->new_data_part->checksums.files.erase(rename_from);
            }
        }

        MutationHelpers::finalizeMutatedPart(ctx->source_part, ctx->new_data_part, ctx->execute_ttl_type, ctx->compression_codec, ctx->context, ctx->metadata_snapshot, ctx->need_sync, ctx->rebuild_vector_index_column);
    }

    enum class State : uint8_t
    {
        NEED_PREPARE,
        NEED_EXECUTE,
        NEED_FINALIZE,

        SUCCESS
    };

    State state{State::NEED_PREPARE};
    MutationContextPtr ctx;
    MergedColumnOnlyOutputStreamPtr out;

    std::unique_ptr<PartMergerWriter> part_merger_writer_task{nullptr};
};


MutateTask::MutateTask(
    FutureMergedMutatedPartPtr future_part_,
    StorageMetadataPtr metadata_snapshot_,
    MutationCommandsConstPtr commands_,
    MergeListEntry * mutate_entry_,
    time_t time_of_mutation_,
    ContextPtr context_,
    ReservationSharedPtr space_reservation_,
    TableLockHolder & table_lock_holder_,
    const MergeTreeTransactionPtr & txn,
    MergeTreeData & data_,
    MergeTreeDataMergerMutator & mutator_,
    ActionBlocker & merges_blocker_,
    bool need_prefix_)
    : ctx(std::make_shared<MutationContext>())
{
    ctx->data = &data_;
    ctx->mutator = &mutator_;
    ctx->merges_blocker = &merges_blocker_;
    ctx->holder = &table_lock_holder_;
    ctx->mutate_entry = mutate_entry_;
    ctx->commands = commands_;
    ctx->context = context_;
    ctx->time_of_mutation = time_of_mutation_;
    ctx->future_part = future_part_;
    ctx->metadata_snapshot = metadata_snapshot_;
    ctx->space_reservation = space_reservation_;
    ctx->storage_columns = metadata_snapshot_->getColumns().getAllPhysical();
    ctx->txn = txn;
    ctx->source_part = ctx->future_part->parts[0];
    ctx->need_prefix = need_prefix_;

    auto storage_snapshot = ctx->data->getStorageSnapshotWithoutData(ctx->metadata_snapshot, context_);
    extendObjectColumns(ctx->storage_columns, storage_snapshot->object_columns, /*with_subcolumns=*/ false);
}


bool MutateTask::execute()
{
    Stopwatch watch;
    SCOPE_EXIT({ ctx->execute_elapsed_ns += watch.elapsedNanoseconds(); });

    switch (state)
    {
        case State::NEED_PREPARE:
        {
            if (!prepare())
                return false;

            state = State::NEED_EXECUTE;
            return true;
        }
        case State::NEED_EXECUTE:
        {
            MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry);

            if (task->executeStep())
                return true;

            // The `new_data_part` is a shared pointer and must be moved to allow
            // part deletion in case it is needed in `MutateFromLogEntryTask::finalize`.
            //
            // `tryRemovePartImmediately` requires `std::shared_ptr::unique() == true`
            // to delete the part timely. When there are multiple shared pointers,
            // only the part state is changed to `Deleting`.
            //
            // Fetching a byte-identical part (in case of checksum mismatches) will fail with
            // `Part ... should be deleted after previous attempt before fetch`.
            promise.set_value(std::move(ctx->new_data_part));
            return false;
        }
    }
    return false;
}

void MutateTask::updateProfileEvents() const
{
    UInt64 total_elapsed_ms = (*ctx->mutate_entry)->watch.elapsedMilliseconds();
    UInt64 execute_elapsed_ms = ctx->execute_elapsed_ns / 1000000UL;

    ProfileEvents::increment(ProfileEvents::MutationTotalMilliseconds, total_elapsed_ms);
    ProfileEvents::increment(ProfileEvents::MutationExecuteMilliseconds, execute_elapsed_ms);
}

static bool canSkipConversionToNullable(const MergeTreeDataPartPtr & part, const MutationCommand & command)
{
    if (command.type != MutationCommand::READ_COLUMN)
        return false;

    auto part_column = part->tryGetColumn(command.column_name);
    if (!part_column)
        return false;

    /// For ALTER MODIFY COLUMN from 'Type' to 'Nullable(Type)' we can skip mutation and
    /// apply only metadata conversion. But it doesn't work for custom serialization.
    const auto * to_nullable = typeid_cast<const DataTypeNullable *>(command.data_type.get());
    if (!to_nullable)
        return false;

    if (!part_column->type->equals(*to_nullable->getNestedType()))
        return false;

    auto serialization = part->getSerialization(command.column_name);
    if (serialization->getKind() != ISerialization::Kind::DEFAULT)
        return false;

    return true;
}

static bool canSkipConversionToVariant(const MergeTreeDataPartPtr & part, const MutationCommand & command)
{
    if (command.type != MutationCommand::READ_COLUMN)
        return false;

    auto part_column = part->tryGetColumn(command.column_name);
    if (!part_column)
        return false;

    /// For ALTER MODIFY COLUMN with Variant extension (like 'Variant(T1, T2)' to 'Variant(T1, T2, T3, ...)')
    /// we can skip mutation and apply only metadata conversion.
    return isVariantExtension(part_column->type, command.data_type);
}

static bool canSkipMutationCommandForPart(const MergeTreeDataPartPtr & part, const MutationCommand & command, const ContextPtr & context)
{
    if (command.partition)
    {
        auto command_partition_id = part->storage.getPartitionIDFromQuery(command.partition, context);
        if (part->info.partition_id != command_partition_id)
            return true;
    }

    if (command.type == MutationCommand::APPLY_DELETED_MASK && !part->hasLightweightDelete())
        return true;

    if (canSkipConversionToNullable(part, command))
        return true;

    if (canSkipConversionToVariant(part, command))
        return true;

    return false;
}

bool MutateTask::prepare()
{
    ProfileEvents::increment(ProfileEvents::MutationTotalParts);
    MutationHelpers::checkOperationIsNotCanceled(*ctx->merges_blocker, ctx->mutate_entry);

    if (ctx->future_part->parts.size() != 1)
        throw Exception(ErrorCodes::LOGICAL_ERROR, "Trying to mutate {} parts, not one. "
            "This is a bug.", ctx->future_part->parts.size());

    ctx->num_mutations = std::make_unique<CurrentMetrics::Increment>(CurrentMetrics::PartMutation);

    auto context_for_reading = Context::createCopy(ctx->context);

    /// Allow mutations to work when force_index_by_date or force_primary_key is on.
    context_for_reading->setSetting("force_index_by_date", false);
    context_for_reading->setSetting("force_primary_key", false);
    context_for_reading->setSetting("apply_mutations_on_fly", false);
    /// Skip using large sets in KeyCondition
    context_for_reading->setSetting("use_index_for_in_with_subqueries_max_values", 100000);

    /// Optimized lightweight can be applied to wide part only.
    ctx->is_optimized_lwd = ctx->commands->empty() ? false : ctx->commands->front().type == MutationCommand::Type::LIGHTWEIGHT_DELETE;
    if (ctx->is_optimized_lwd && (!isWidePart(ctx->source_part) || !isFullPartStorage(ctx->source_part->getDataPartStorage())))
        ctx->is_optimized_lwd = false;

    for (const auto & command : *ctx->commands)
    {
        if (!canSkipMutationCommandForPart(ctx->source_part, command, context_for_reading))
        {
            if (command.type == MutationCommand::Type::LIGHTWEIGHT_DELETE && !ctx->is_optimized_lwd)
            {
                /// Use "UPDATE _row_exists = 0 WHERE predicate" for compact part
                auto assignment = std::make_shared<ASTAssignment>();
                assignment->column_name = RowExistsColumn::name;
                assignment->children.push_back(std::make_shared<ASTLiteral>(0UL));

                auto command_update_assignments = std::make_shared<ASTExpressionList>();
                command_update_assignments->children.emplace_back(std::move(assignment));

                auto update_cmd = std::make_shared<ASTAlterCommand>();
                update_cmd->type = ASTAlterCommand::Type::UPDATE;
                update_cmd->update_assignments = update_cmd->children.emplace_back(std::move(command_update_assignments)).get();
                update_cmd->predicate = update_cmd->children.emplace_back(command.predicate).get();

                auto mutation_command = MutationCommand::parse(update_cmd.get(), true);
                ctx->commands_for_part.push_back(std::move(*mutation_command));
            }
            else
                ctx->commands_for_part.emplace_back(command);

            /// lightweight delete is changed to update command.
            /// Currently delete and TTL will delete rows.
            if (!ctx->need_delete_rows && (command.type == MutationCommand::Type::DELETE ||
                    command.type == MutationCommand::Type::MATERIALIZE_TTL))
                ctx->need_delete_rows = true;
        }
    }
    ctx->move_index_read_lock = ctx->source_part->segments_mgr->tryLockSegmentsTimed(RWLockImpl::Type::Read, std::chrono::milliseconds(1000));
    ctx->rebuild_vector_index_column = MutationHelpers::getVectorIndicesToRebuild(*ctx->commands);
    ctx->hardlink_vector_index_files = VectorIndex::getAllValidVectorIndexFileNames(*ctx->source_part);
    /// Avoid to call isStorageTouchedByMutations() for optimized lightweight delete, instead get the deleted row ids.
    bool is_storage_touched_by_mutations = true;
    if (ctx->source_part->isStoredOnDisk())
    {
        if (ctx->is_optimized_lwd)
            is_storage_touched_by_mutations = isStorageTouchedByLWDMutations(ctx->source_part,
                    ctx->metadata_snapshot, ctx->commands_for_part, context_for_reading, ctx->current_task_deleted_row_ids);
        else
            is_storage_touched_by_mutations = isStorageTouchedByMutations(
                    ctx->source_part, ctx->metadata_snapshot, ctx->commands_for_part, context_for_reading);
    }

    if (ctx->source_part->isStoredOnDisk() && !is_storage_touched_by_mutations)
    {
        NameSet files_to_copy_instead_of_hardlinks;
        auto settings_ptr = ctx->data->getSettings();
        /// In zero-copy replication checksums file path in s3 (blob path) is used for zero copy locks in ZooKeeper. If we will hardlink checksums file, we will have the same blob path
        /// and two different parts (source and new mutated part) will use the same locks in ZooKeeper. To avoid this we copy checksums.txt to generate new blob path.
        /// Example:
        ///     part: all_0_0_0/checksums.txt -> /s3/blobs/shjfgsaasdasdasdasdasdas
        ///     locks path in zk: /zero_copy/tbl_id/s3_blobs_shjfgsaasdasdasdasdasdas/replica_name
        ///                                         ^ part name don't participate in lock path
        /// In case of full hardlink we will have:
        ///     part: all_0_0_0_1/checksums.txt -> /s3/blobs/shjfgsaasdasdasdasdasdas
        ///     locks path in zk: /zero_copy/tbl_id/s3_blobs_shjfgsaasdasdasdasdasdas/replica_name
        /// So we need to copy to have a new name
        bool copy_checksumns = ctx->data->supportsReplication() && settings_ptr->allow_remote_fs_zero_copy_replication && ctx->source_part->isStoredOnRemoteDiskWithZeroCopySupport();
        if (copy_checksumns)
            files_to_copy_instead_of_hardlinks.insert(IMergeTreeDataPart::FILE_FOR_REFERENCES_CHECK);

        LOG_TRACE(ctx->log, "Part {} doesn't change up to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
        std::string prefix;
        if (ctx->need_prefix)
            prefix = "tmp_clone_";

        IDataPartStorage::ClonePartParams clone_params
        {
            .txn = ctx->txn, .hardlinked_files = &ctx->hardlinked_files,
            .copy_instead_of_hardlink = settings_ptr->always_use_copy_instead_of_hardlinks,
            .files_to_copy_instead_of_hardlinks = std::move(files_to_copy_instead_of_hardlinks),
            .keep_metadata_version = true,
        };
        MergeTreeData::MutableDataPartPtr part;
        scope_guard lock;

        {
            std::tie(part, lock) = ctx->data->cloneAndLoadDataPart(
                ctx->source_part, prefix, ctx->future_part->part_info, ctx->metadata_snapshot, clone_params, ctx->context->getReadSettings(), ctx->context->getWriteSettings(), true/*must_on_same_disk*/);

            /// Inherit index status from source part
            part->segments_mgr->mutateFrom(*ctx->source_part->segments_mgr, ctx->rebuild_vector_index_column);

            part->getDataPartStorage().beginTransaction();
#if USE_TANTIVY_SEARCH
        auto metadata = ctx->source_part->storage.getInMemoryMetadataPtr();
        if (metadata->hasSecondaryIndices() && metadata->getSecondaryIndices().hasFTS())
        {
            TantivyIndexStoreFactory::instance().mutate(
                ctx->source_part->getDataPartStoragePtr()->getRelativePath(), part->getDataPartStoragePtr()->getRelativePath());
        }
#endif
            ctx->temporary_directory_lock = std::move(lock);
        }

        ProfileEvents::increment(ProfileEvents::MutationUntouchedParts);
        promise.set_value(std::move(part));
        return false;
    }
    else
    {
        LOG_TRACE(ctx->log, "Mutating part {} to mutation version {}", ctx->source_part->name, ctx->future_part->part_info.mutation);
    }

    /// We must read with one thread because it guarantees that output stream will be sorted.
    /// Disable all settings that can enable reading with several streams.
    /// NOTE: isStorageTouchedByMutations() above is done without this settings because it
    /// should be ok to calculate count() with multiple streams.
    context_for_reading->setSetting("max_streams_to_max_threads_ratio", 1);
    context_for_reading->setSetting("max_threads", 1);
    context_for_reading->setSetting("allow_asynchronous_read_from_io_pool_for_merge_tree", false);
    context_for_reading->setSetting("max_streams_for_merge_tree_reading", Field(0));
    context_for_reading->setSetting("read_from_filesystem_cache_if_exists_otherwise_bypass_cache", 1);

    MutationHelpers::splitAndModifyMutationCommands(
        ctx->source_part, ctx->metadata_snapshot,
        ctx->commands_for_part, ctx->for_interpreter, ctx->for_file_renames, ctx->log);

#if USE_TANTIVY_SEARCH
    MutationHelpers::removeTantivyIndexCache(ctx->source_part, ctx->for_file_renames);
#endif

    ctx->stage_progress = std::make_unique<MergeStageProgress>(1.0);

    if (!ctx->for_interpreter.empty())
    {
        /// Always disable filtering in mutations: we want to read and write all rows because for updates we rewrite only some of the
        /// columns and preserve the columns that are not affected, but after the update all columns must have the same number of row
        MutationsInterpreter::Settings settings(true);
        settings.apply_deleted_mask = false;

        ctx->interpreter = std::make_unique<MutationsInterpreter>(
            *ctx->data, ctx->source_part, ctx->metadata_snapshot, ctx->for_interpreter,
            ctx->metadata_snapshot->getColumns().getNamesOfPhysical(), context_for_reading, settings);

        ctx->materialized_indices = ctx->interpreter->grabMaterializedIndices();
        ctx->materialized_statistics = ctx->interpreter->grabMaterializedStatistics();
        ctx->materialized_projections = ctx->interpreter->grabMaterializedProjections();
        ctx->mutating_pipeline_builder = ctx->interpreter->execute();
        ctx->updated_header = ctx->interpreter->getUpdatedHeader();
        ctx->progress_callback = MergeProgressCallback((*ctx->mutate_entry)->ptr(), ctx->watch_prev_elapsed, *ctx->stage_progress);
    }

    auto single_disk_volume = std::make_shared<SingleDiskVolume>("volume_" + ctx->future_part->name, ctx->space_reservation->getDisk(), 0);

    std::string prefix;
    if (ctx->need_prefix)
        prefix = "tmp_mut_";
    String tmp_part_dir_name = prefix + ctx->future_part->name;
    ctx->temporary_directory_lock = ctx->data->getTemporaryPartDirectoryHolder(tmp_part_dir_name);

    auto builder = ctx->data->getDataPartBuilder(ctx->future_part->name, single_disk_volume, tmp_part_dir_name);
    builder.withPartFormat(ctx->future_part->part_format);
    builder.withPartInfo(ctx->future_part->part_info);

    ctx->new_data_part = std::move(builder).build();
    ctx->new_data_part->getDataPartStorage().beginTransaction();

    ctx->new_data_part->uuid = ctx->future_part->uuid;
    ctx->new_data_part->is_temp = true;
    ctx->new_data_part->ttl_infos = ctx->source_part->ttl_infos;

    /// It shouldn't be changed by mutation.
    ctx->new_data_part->index_granularity_info = ctx->source_part->index_granularity_info;

    for (const auto & updated_col_name : ctx->updated_header.getNames())
    {
        LOG_DEBUG(ctx->log, "updated column name: {}", updated_col_name);
    }

    auto [new_columns, new_infos] = MutationHelpers::getColumnsForNewDataPart(
        ctx->source_part, ctx->updated_header, ctx->storage_columns,
        ctx->source_part->getSerializationInfos(), ctx->for_interpreter, ctx->for_file_renames);

    ctx->new_data_part->setColumns(new_columns, new_infos, ctx->metadata_snapshot->getMetadataVersion());
    ctx->new_data_part->partition.assign(ctx->source_part->partition);

    /// Don't change granularity type while mutating subset of columns
    ctx->mrk_extension = ctx->source_part->index_granularity_info.mark_type.getFileExtension();

    const auto data_settings = ctx->data->getSettings();
    ctx->need_sync = needSyncPart(ctx->source_part->rows_count, ctx->source_part->getBytesOnDisk(), *data_settings);
    ctx->execute_ttl_type = ExecuteTTLType::NONE;

    if (ctx->mutating_pipeline_builder.initialized())
        ctx->execute_ttl_type = MutationHelpers::shouldExecuteTTL(ctx->metadata_snapshot, ctx->interpreter->getColumnDependencies());

    if (ctx->data->getSettings()->exclude_deleted_rows_for_part_size_in_merge && ctx->updated_header.has(RowExistsColumn::name))
    {
        /// This mutation contains lightweight delete and we need to count the deleted rows,
        /// Reset existing_rows_count of new data part to 0 and it will be updated while writing _row_exists column
        ctx->count_lightweight_deleted_rows = true;
    }
    else
    {
        ctx->count_lightweight_deleted_rows = false;

        /// No need to count deleted rows, copy existing_rows_count from source part
        ctx->new_data_part->existing_rows_count = ctx->source_part->existing_rows_count.value_or(ctx->source_part->rows_count);
    }

    if (ctx->updated_header.has(RowExistsColumn::name))
    {
        /// Check if lightweight delete mask column is updated.
        /// If true, mark lightweight delete mask updated to true. Will trigger vector index bitmap update.
        /// Support part with simple built index and decoupled part with merged old parts' built index files
        /// When any normal delete or ttl command exists, needs to be build vector index for the new data part.
        if (!ctx->need_delete_rows)
            ctx->new_data_part->setDeletedMaskUpdated(true);
    }

    /// All columns from part are changed and may be some more that were missing before in part
    /// TODO We can materialize compact part without copying data
    /// Also currently mutations of types with dynamic subcolumns in Wide part are possible only by
    /// rewriting the whole part.
    if (MutationHelpers::haveMutationsOfDynamicColumns(ctx->source_part, ctx->commands_for_part) || !isWidePart(ctx->source_part) || !isFullPartStorage(ctx->source_part->getDataPartStorage())
        || (ctx->interpreter && ctx->interpreter->isAffectingAllColumns()))
    {
        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse claims that this new part can be deleted in temporary state without unlocking the blobs
        /// The blobs have to be removed along with the part, this temporary part owns them and does not share them yet.
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::REMOVE_BLOBS;

        task = std::make_unique<MutateAllPartColumnsTask>(ctx);
        ProfileEvents::increment(ProfileEvents::MutationAllPartColumns);
    }
    else /// TODO: check that we modify only non-key columns in this case.
    {
        ctx->indices_to_recalc = MutationHelpers::getIndicesToRecalculate(
            ctx->source_part,
            ctx->mutating_pipeline_builder,
            ctx->metadata_snapshot,
            ctx->context,
            ctx->materialized_indices);

        ctx->projections_to_recalc = MutationHelpers::getProjectionsToRecalculate(
            ctx->source_part,
            ctx->metadata_snapshot,
            ctx->materialized_projections);

        ctx->stats_to_recalc = MutationHelpers::getStatisticsToRecalculate(ctx->metadata_snapshot, ctx->materialized_statistics);

        ctx->files_to_skip = MutationHelpers::collectFilesToSkip(
            ctx->source_part,
            ctx->new_data_part,
            ctx->updated_header,
            ctx->indices_to_recalc,
            ctx->mrk_extension,
            ctx->projections_to_recalc,
            ctx->stats_to_recalc);

        ctx->files_to_rename = MutationHelpers::collectFilesForRenames(
            ctx->source_part,
            ctx->new_data_part,
            ctx->for_file_renames,
            ctx->mrk_extension);

        /// In case of replicated merge tree with zero copy replication
        /// Here Clickhouse has to follow the common procedure when deleting new part in temporary state
        /// Some of the files within the blobs are shared with source part, some belongs only to the part
        /// Keeper has to be asked with unlock request to release the references to the blobs
        ctx->new_data_part->remove_tmp_policy = IMergeTreeDataPart::BlobsRemovalPolicyForTemporaryParts::ASK_KEEPER;

        task = std::make_unique<MutateSomePartColumnsTask>(ctx);
        ProfileEvents::increment(ProfileEvents::MutationSomePartColumns);
    }

    return true;
}

const HardlinkedFiles & MutateTask::getHardlinkedFiles() const
{
    return ctx->hardlinked_files;
}

}
