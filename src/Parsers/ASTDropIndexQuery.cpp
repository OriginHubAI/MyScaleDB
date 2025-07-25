#include <Common/quoteString.h>
#include <IO/Operators.h>
#include <Parsers/ASTDropIndexQuery.h>


namespace DB
{

/** Get the text that identifies this element. */
String ASTDropIndexQuery::getID(char delim) const
{
    return "DropIndexQuery" + (delim + getDatabase()) + delim + getTable();
}

ASTPtr ASTDropIndexQuery::clone() const
{
    auto res = std::make_shared<ASTDropIndexQuery>(*this);
    res->children.clear();

    res->index_name = index_name->clone();
    res->children.push_back(res->index_name);

    cloneTableOptions(*res);

    return res;
}

void ASTDropIndexQuery::formatQueryImpl(const FormatSettings & settings, FormatState & state, FormatStateStacked frame) const
{
    frame.need_parens = false;

    std::string indent_str = settings.one_line ? "" : std::string(4u * frame.indent, ' ');

    settings.ostr << (settings.hilite ? hilite_keyword : "") << indent_str;

    settings.ostr << "DROP " << (is_vector_index ? "VECTOR " : "");
    settings.ostr << "INDEX " << (if_exists ? "IF EXISTS " : "");
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
}

ASTPtr ASTDropIndexQuery::convertToASTAlterCommand() const
{
    auto command = std::make_shared<ASTAlterCommand>();
    command->if_exists = if_exists;
    if (is_vector_index)
    {
        command->type = ASTAlterCommand::DROP_VECTOR_INDEX;
        command->vec_index = command->children.emplace_back(index_name).get();
    }
    else
    {
        command->type = ASTAlterCommand::DROP_INDEX;
        command->index = command->children.emplace_back(index_name).get();
    }

    return command;
}

}
