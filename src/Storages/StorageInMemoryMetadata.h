#pragma once

#include <Parsers/Access/ASTUserNameWithHost.h>
#include <Parsers/ASTCreateQuery.h>
#include <Parsers/IAST_fwd.h>
#include <Storages/ColumnDependency.h>
#include <Storages/ColumnsDescription.h>
#include <Storages/ConstraintsDescription.h>
#include <Storages/IndicesDescription.h>
#include <Storages/KeyDescription.h>
#include <Storages/ProjectionsDescription.h>
#include <Storages/SelectQueryDescription.h>
#include <Storages/TTLDescription.h>
#include <Storages/MaterializedView/RefreshSchedule.h>

#include <VectorIndex/Storages/VIDescriptions.h>
#include <VectorIndex/Utils/CommonUtils.h>
#include <Common/MultiVersion.h>

namespace DB
{

/// Common metadata for all storages. Contains all possible parts of CREATE
/// query from all storages, but only some subset used.
struct StorageInMemoryMetadata
{
    /// Columns of table with their names, types,
    /// defaults, comments, etc. All table engines have columns.
    ColumnsDescription columns;
    /// Table indices. Currently supported for MergeTree only.
    IndicesDescription secondary_indices;
    /// Vector indices.
    VIDescriptions vec_indices;
    /// Table constraints. Currently supported for MergeTree only.
    ConstraintsDescription constraints;
    /// Table projections. Currently supported for MergeTree only.
    ProjectionsDescription projections;
    /// Table minmax_count projection. Currently supported for MergeTree only.
    std::optional<ProjectionDescription> minmax_count_projection;
    /// PARTITION BY expression. Currently supported for MergeTree only.
    KeyDescription partition_key;
    /// PRIMARY KEY expression. If absent, than equal to order_by_ast.
    KeyDescription primary_key;
    /// ORDER BY expression. Required field for all MergeTree tables
    /// even in old syntax MergeTree(partition_key, order_by, ...)
    KeyDescription sorting_key;
    /// SAMPLE BY expression. Supported for MergeTree only.
    KeyDescription sampling_key;
    /// Separate ttl expressions for columns
    TTLColumnsDescription column_ttls_by_name;
    /// TTL expressions for table (Move and Rows)
    TTLTableDescription table_ttl;
    /// SETTINGS expression. Supported for MergeTree, Buffer, Kafka, RabbitMQ.
    ASTPtr settings_changes;
    /// SELECT QUERY. Supported for MaterializedView and View (have to support LiveView).
    SelectQueryDescription select;
    /// Materialized view REFRESH parameters.
    ASTPtr refresh;

    /// DEFINER <user_name>. Allows to specify a definer of the table.
    /// Supported for MaterializedView and View.
    std::optional<String> definer;

    /// SQL SECURITY <DEFINER | INVOKER | NONE>
    /// Supported for MaterializedView and View.
    std::optional<SQLSecurityType> sql_security_type;

    String comment;

    /// Version of metadata. Managed properly by ReplicatedMergeTree only
    /// (zero-initialization is important)
    int32_t metadata_version = 0;

    StorageInMemoryMetadata() = default;

    StorageInMemoryMetadata(const StorageInMemoryMetadata & other);
    StorageInMemoryMetadata & operator=(const StorageInMemoryMetadata & other);

    StorageInMemoryMetadata(StorageInMemoryMetadata && other) = default; /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)
    StorageInMemoryMetadata & operator=(StorageInMemoryMetadata && other) = default; /// NOLINT(hicpp-noexcept-move,performance-noexcept-move-constructor)

    /// NOTE: Thread unsafe part. You should not modify same StorageInMemoryMetadata
    /// structure from different threads. It should be used as MultiVersion
    /// object. See example in IStorage.

    /// Sets a user-defined comment for a table
    void setComment(const String & comment_);

    /// Sets only real columns, possibly overwrites virtual ones.
    void setColumns(ColumnsDescription columns_);

    /// Sets secondary indices
    void setSecondaryIndices(IndicesDescription secondary_indices_);

    void setVectorIndices(VIDescriptions vec_indices_);

    /// Sets constraints
    void setConstraints(ConstraintsDescription constraints_);

    /// Sets projections
    void setProjections(ProjectionsDescription projections_);

    /// Set common table TTLs
    void setTableTTLs(const TTLTableDescription & table_ttl_);

    /// TTLs for separate columns
    void setColumnTTLs(const TTLColumnsDescription & column_ttls_by_name_);

    /// Set settings changes in metadata (some settings exlicetely specified in
    /// CREATE query)
    void setSettingsChanges(const ASTPtr & settings_changes_);

    /// Set SELECT query for (Materialized)View
    void setSelectQuery(const SelectQueryDescription & select_);

    /// Set refresh parameters for materialized view (REFRESH ... [DEPENDS ON ...] [SETTINGS ...]).
    void setRefresh(ASTPtr refresh_);

    /// Set version of metadata.
    void setMetadataVersion(int32_t metadata_version_);
    /// Get copy of current metadata with metadata_version_
    StorageInMemoryMetadata withMetadataVersion(int32_t metadata_version_) const;

    /// Sets SQL security for the storage.
    void setSQLSecurity(const ASTSQLSecurity & sql_security);
    UUID getDefinerID(ContextPtr context) const;

    /// Returns a copy of the context with the correct user from SQL security options.
    /// If the SQL security wasn't set, this is equivalent to `Context::createCopy(context)`.
    /// The context from this function must be used every time whenever views execute any read/write operations or subqueries.
    ContextMutablePtr getSQLSecurityOverriddenContext(ContextPtr context) const;

    /// Returns combined set of columns
    const ColumnsDescription & getColumns() const;

    /// Returns secondary indices
    const IndicesDescription & getSecondaryIndices() const;

    /// Has at least one non primary index
    bool hasSecondaryIndices() const;

    /// Returns vector indices
    const VIDescriptions & getVectorIndices() const;

    /// Has at least one vector index
    bool hasVectorIndices() const;

    /// Has vector index defined on column
    bool hasVectorIndexOnColumn(const String & column_name) const;

    /// Return table constraints
    const ConstraintsDescription & getConstraints() const;

    const ProjectionsDescription & getProjections() const;

    /// Has at least one projection
    bool hasProjections() const;

    /// Returns true if there is set table TTL, any column TTL or any move TTL.
    bool hasAnyTTL() const { return hasAnyColumnTTL() || hasAnyTableTTL(); }

    /// Common tables TTLs (for rows and moves).
    TTLTableDescription getTableTTLs() const;
    bool hasAnyTableTTL() const;

    /// Separate TTLs for columns.
    TTLColumnsDescription getColumnTTLs() const;
    bool hasAnyColumnTTL() const;

    /// Just wrapper for table TTLs, return rows part of table TTLs.
    TTLDescription getRowsTTL() const;
    bool hasRowsTTL() const;

    TTLDescriptions getRowsWhereTTLs() const;
    bool hasAnyRowsWhereTTL() const;

    /// Just wrapper for table TTLs, return moves (to disks or volumes) parts of
    /// table TTL.
    TTLDescriptions getMoveTTLs() const;
    bool hasAnyMoveTTL() const;

    // Just wrapper for table TTLs, return info about recompression ttl
    TTLDescriptions getRecompressionTTLs() const;
    bool hasAnyRecompressionTTL() const;

    // Just wrapper for table TTLs, return info about recompression ttl
    TTLDescriptions getGroupByTTLs() const;
    bool hasAnyGroupByTTL() const;

    using HasDependencyCallback = std::function<bool(const String &, ColumnDependency::Kind)>;

    /// Returns columns, which will be needed to calculate dependencies (skip indices, projections,
    /// TTL expressions) if we update @updated_columns set of columns.
    ColumnDependencies getColumnDependencies(
        const NameSet & updated_columns,
        bool include_ttl_target,
        const HasDependencyCallback & has_dependency) const;

    /// Block with ordinary + materialized columns.
    Block getSampleBlock() const;

    /// Block with ordinary + ephemeral.
    Block getSampleBlockInsertable() const;

    /// Block with ordinary columns.
    Block getSampleBlockNonMaterialized() const;

    /// Block with ordinary + materialized + virtuals. Virtuals have to be
    /// explicitly specified, because they are part of Storage type, not
    /// Storage metadata.
    Block getSampleBlockWithVirtuals(const NamesAndTypesList & virtuals) const;

    /// Returns structure with partition key.
    const KeyDescription & getPartitionKey() const;
    /// Returns ASTExpressionList of partition key expression for storage or nullptr if there is none.
    ASTPtr getPartitionKeyAST() const { return partition_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) partition key.
    bool isPartitionKeyDefined() const;
    /// Storage has partition key.
    bool hasPartitionKey() const;
    /// Returns column names that need to be read to calculate partition key.
    Names getColumnsRequiredForPartitionKey() const;

    /// Returns structure with sorting key.
    const KeyDescription & getSortingKey() const;
    /// Returns ASTExpressionList of sorting key expression for storage or nullptr if there is none.
    ASTPtr getSortingKeyAST() const { return sorting_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sorting key.
    bool isSortingKeyDefined() const;
    /// Storage has sorting key. It means, that it contains at least one column.
    bool hasSortingKey() const;
    /// Returns column names that need to be read to calculate sorting key.
    Names getColumnsRequiredForSortingKey() const;
    /// Returns columns names in sorting key specified by user in ORDER BY
    /// expression. For example: 'a', 'x * y', 'toStartOfMonth(date)', etc.
    Names getSortingKeyColumns() const;

    /// Returns column names that need to be read for FINAL to work.
    Names getColumnsRequiredForFinal() const { return getColumnsRequiredForSortingKey(); }

    /// Returns structure with sampling key.
    const KeyDescription & getSamplingKey() const;
    /// Returns sampling expression AST for storage or nullptr if there is none.
    ASTPtr getSamplingKeyAST() const { return sampling_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sampling key.
    bool isSamplingKeyDefined() const;
    /// Storage has sampling key.
    bool hasSamplingKey() const;
    /// Returns column names that need to be read to calculate sampling key.
    Names getColumnsRequiredForSampling() const;

    /// Returns structure with primary key.
    const KeyDescription & getPrimaryKey() const;
    /// Returns ASTExpressionList of primary key expression for storage or nullptr if there is none.
    ASTPtr getPrimaryKeyAST() const { return primary_key.definition_ast; }
    /// Storage has user-defined (in CREATE query) sorting key.
    bool isPrimaryKeyDefined() const;
    /// Storage has primary key (maybe part of some other key). It means, that
    /// it contains at least one column.
    bool hasPrimaryKey() const;
    /// Returns column names that need to be read to calculate primary key.
    Names getColumnsRequiredForPrimaryKey() const;
    /// Returns columns names in sorting key specified by. For example: 'a', 'x
    /// * y', 'toStartOfMonth(date)', etc.
    Names getPrimaryKeyColumns() const;

    /// Storage settings
    ASTPtr getSettingsChanges() const;
    bool hasSettingsChanges() const { return settings_changes != nullptr; }

    /// Select query for *View storages.
    const SelectQueryDescription & getSelectQuery() const;
    bool hasSelectQuery() const;

    /// Get version of metadata
    int32_t getMetadataVersion() const { return metadata_version; }

    /// Check that all the requested names are in the table and have the correct types.
    void check(const NamesAndTypesList & columns) const;

    /// Check that all names from the intersection of `names` and `columns` are in the table and have the same types.
    void check(const NamesAndTypesList & columns, const Names & column_names) const;

    /// Check that the data block contains all the columns of the table with the correct types,
    /// contains only the columns of the table, and all the columns are different.
    /// If |need_all| is set, then checks that all the columns of the table are in the block.
    void check(const Block & block, bool need_all = false) const;
};

using StorageMetadataPtr = std::shared_ptr<const StorageInMemoryMetadata>;
using MultiVersionStorageMetadataPtr = MultiVersion<StorageInMemoryMetadata>;

String listOfColumns(const NamesAndTypesList & available_columns);

}
