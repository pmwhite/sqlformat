module Syntax

type SelectDistinct =
    | Distinct
    | All

type SelectTopUnit =
    | Count
    | Percent

type Path = Path of string list

type JoinKind =
    | Left
    | Right
    | Outer

type FunctionCall =
    { function_name: Path
      parameters: Expression list }

and AsExpression =
    { name: string
      expression: Expression }

and InExpression =
    { condition: Expression
      subquery: SelectStatement }

and Expression =
    | Literal of string
    | Name of Path
    | Asterisk of Path
    | Call of FunctionCall
    | As of AsExpression
    | In of InExpression
    | Equals of Expression * Expression

and SelectTop =
    { amount: Expression
      unit: SelectTopUnit
      with_ties: bool }

and Join =
    { kind: JoinKind
      on: Expression
      table: Expression }

and SelectFrom = { joins: Join list; table: Expression }

and SelectStatement =
    { distinct: SelectDistinct option
      select_list: Expression list
      from: SelectFrom
      where: Expression option
      top: SelectTop option }

type CommonTableExpression =
    { name: string
      columns: string list option
      query: SelectStatement }

type DmlClauseBody = Select of SelectStatement

type DmlClause =
    { locals: CommonTableExpression list
      body: DmlClauseBody }

type SqlClause = Dml of DmlClause

type GoStatement = { count: string option }

(* TODO there is a bit more to batches than just a list of sql clauses. *)
type Batch =
    | Go of GoStatement
    | Sql of SqlClause list

type File = Batches of Batch list
