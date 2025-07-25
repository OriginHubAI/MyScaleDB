#pragma once

#include <Parsers/IParserBase.h>
#include <Parsers/ExpressionElementParsers.h>
#include <Parsers/ASTAlterQuery.h>

namespace DB
{

/** Query like this:
  * ALTER TABLE [db.]name [ON CLUSTER cluster]
  *     [ADD COLUMN [IF NOT EXISTS] col_name type [AFTER col_after],]
  *     [DROP COLUMN [IF EXISTS] col_to_drop, ...]
  *     [CLEAR COLUMN [IF EXISTS] col_to_clear[ IN PARTITION partition],]
  *     [MODIFY COLUMN [IF EXISTS] col_to_modify type, ...]
  *     [RENAME COLUMN [IF EXISTS] col_name TO col_name]
  *     [MODIFY PRIMARY KEY (a, b, c...)]
  *     [MODIFY SETTING setting_name=setting_value, ...]
  *     [RESET SETTING setting_name, ...]
  *     [COMMENT COLUMN [IF EXISTS] col_name string]
  *     [MODIFY COMMENT string]
  *     [DROP|DETACH|ATTACH PARTITION|PART partition, ...]
  *     [FETCH PARTITION partition FROM ...]
  *     [FREEZE [PARTITION] [WITH NAME name]]
  *     [DELETE[ IN PARTITION partition] WHERE ...]
  *     [UPDATE col_name = expr, ...[ IN PARTITION partition] WHERE ...]
  *     [ADD INDEX [IF NOT EXISTS] index_name [AFTER index_name]]
  *     [DROP INDEX [IF EXISTS] index_name]
  *     [CLEAR INDEX [IF EXISTS] index_name IN PARTITION partition]
  *     [MATERIALIZE INDEX [IF EXISTS] index_name [IN PARTITION partition]]
  *     [ADD VECTOR INDEX [IF NOT EXISTS] index_name]
  *     [DROP VECTOR INDEX [IF EXISTS] index_name]
  */

class ParserAlterQuery : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER query"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;
};


class ParserAlterCommandList : public IParserBase
{
protected:
    const char * getName() const  override{ return "a list of ALTER commands"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    ASTAlterQuery::AlterObjectType alter_object;

    explicit ParserAlterCommandList(ASTAlterQuery::AlterObjectType alter_object_ = ASTAlterQuery::AlterObjectType::TABLE)
        : alter_object(alter_object_) {}
};


class ParserAlterCommand : public IParserBase
{
protected:
    const char * getName() const  override{ return "ALTER command"; }
    bool parseImpl(Pos & pos, ASTPtr & node, Expected & expected) override;

public:
    bool with_round_bracket;
    ASTAlterQuery::AlterObjectType alter_object;


    explicit ParserAlterCommand(
        bool with_round_bracket_, ASTAlterQuery::AlterObjectType alter_object_ = ASTAlterQuery::AlterObjectType::TABLE)
        : with_round_bracket(with_round_bracket_), alter_object(alter_object_)
    {
    }
};


}
