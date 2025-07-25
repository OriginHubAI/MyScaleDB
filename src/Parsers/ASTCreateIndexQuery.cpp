#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTCreateIndexQuery.h>
#include <Parsers/ASTIndexDeclaration.h>
#include <Parsers/ASTAlterQuery.h>
#include <VectorIndex/Parsers/ASTVIDeclaration.h>


namespace DB
{

/** Get the text that identifies this element. */
String ASTCreateIndexQuery::getID(char delim) const
{
    return "CreateIndexQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTCreateIndexQuery::clone() const
{
    auto res = std::make_shared<ASTCreateIndexQuery>(*this);
    res->children.clear();

    res->index_name = index_name->clone();
    res->children.push_back(res->index_name);

    res->index_decl = index_decl->clone();
    res->children.push_back(res->index_decl);

    cloneTableOptions(*res);

    return res;
}

void ASTCreateIndexQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    settings.ostr << "CREATE " << (unique ? "UNIQUE " : "") << (is_vector_index ? "VECTOR " : "") << "INDEX " << (if_not_exists ? "IF NOT EXISTS " : "");
    index_name->formatImpl(settings, state, frame);
    settings.ostr << " ON ";

    settings.ostr << (settings.hilite ? hilite_none : "");

    if (table)
    {
        if (database)
        {
            database->formatImpl(settings, state, frame);
            settings.ostr << '.';
        }

        chassert(table);
        table->formatImpl(settings, state, frame);
    }

    formatOnCluster(settings);

    settings.ostr << " ";

    index_decl->formatImpl(settings, state, frame);
}

ASTPtr ASTCreateIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();

    command->if_not_exists = if_not_exists;

    if (is_vector_index)
    {
        command->type = ASTAlterCommand::ADD_VECTOR_INDEX;
        command->vec_index = command->children.emplace_back(index_name).get();
        command->vec_index_decl = command->children.emplace_back(index_decl).get();
        /// mark part_of_create_index_query to false
        auto & ast_vec_index_decl = command->vec_index_decl->as<ASTVIDeclaration &>();
        ast_vec_index_decl.part_of_create_index_query = false;
    }
    else
    {
        command->type = ASTAlterCommand::ADD_INDEX;
        command->index = command->children.emplace_back(index_name).get();
        command->index_decl = command->children.emplace_back(index_decl).get();
        /// mark part_of_create_index_query to false
        auto & ast_index_decl = command->index_decl->as<ASTIndexDeclaration &>();
        ast_index_decl.part_of_create_index_query = false;
    }

    return command;
}

}
