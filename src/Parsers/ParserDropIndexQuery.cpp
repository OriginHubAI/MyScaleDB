#include <Parsers/ASTDropIndexQuery.h>
#include <Parsers/ASTIdentifier.h>
#include <Parsers/CommonParsers.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ParserDropIndexQuery.h>
#include <Parsers/parseDatabaseAndTableName.h>

namespace DB
{

bool ParserDropIndexQuery::parseImpl(IParser::Pos & pos, ASTPtr & node, Expected & expected)
{
    auto query = std::make_shared<ASTDropIndexQuery>();
    node = query;

    ParserKeyword s_drop(Keyword::DROP);
    ParserKeyword s_vector(Keyword::VECTOR);
    ParserKeyword s_index(Keyword::INDEX);
    ParserKeyword s_on(Keyword::ON);
    ParserKeyword s_if_exists(Keyword::IF_EXISTS);
    ParserIdentifier index_name_p;

    String cluster_str;
    bool if_exists = false;
    bool is_vector_index = false;

    if (!s_drop.ignore(pos, expected))
        return false;

    if (s_vector.ignore(pos, expected))
        is_vector_index = true;
    
    if (!s_index.ignore(pos, expected))
        return false;

    if (s_if_exists.ignore(pos, expected))
        if_exists = true;

    if (!index_name_p.parse(pos, query->index_name, expected))
        return false;

    /// ON [db.] table_name
    if (!s_on.ignore(pos, expected))
        return false;

    if (!parseDatabaseAndTableAsAST(pos, expected, query->database, query->table))
        return false;

    /// [ON cluster_name]
    if (s_on.ignore(pos, expected))
    {
        if (!ASTQueryWithOnCluster::parse(pos, cluster_str, expected))
            return false;

        query->cluster = std::move(cluster_str);
    }

    if (query->index_name)
        query->children.push_back(query->index_name);

    query->if_exists = if_exists;

    if (query->database)
        query->children.push_back(query->database);

    if (query->table)
        query->children.push_back(query->table);

    query->is_vector_index = is_vector_index;

    return true;
}

}
