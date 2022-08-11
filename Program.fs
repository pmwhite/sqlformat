open Microsoft.SqlServer.TransactSql.ScriptDom
open System.Collections.Generic

#nowarn "40"
#nowarn "3391"

let mutable errors = []
let log_error message = errors <- message :: errors

exception FatalError

let get_filename () =
    let args = System.Environment.GetCommandLineArgs()

    if args.Length <> 2 then
        log_error "This program must be invoked with one argument - the filename to format"
        raise FatalError
    else
        args[1]

let get_file_tokens filename =
    let file_stream =
        try
            System.IO.File.OpenRead filename
        with
        | _ ->
            log_error (sprintf "Failed to open file '%s'. Make sure it exists." filename)
            raise FatalError

    let file_reader = new System.IO.StreamReader(file_stream)
    let parser = TSql160Parser true
    let tokens, token_errors = parser.GetTokenStream file_reader

    for error in token_errors do
        log_error (sprintf "%d:%d\t%s" error.Line error.Column error.Message)

    List.ofSeq tokens

type ParseError =
    { line: int
      column: int
      expected: string
      found: string
      consumed_input: bool }

type 'a Parser = Parser of (TSqlParserToken list -> bool -> Result<'a * TSqlParserToken list * bool, ParseError>)

type ParserBuilder() =
    member x.Bind(Parser parser, f) =
        Parser (fun tokens consumed_input ->
            match parser tokens consumed_input with
            | Ok (result, remaining, consumed_input) ->
                let (Parser next_parser) = f result
                next_parser remaining consumed_input
            | Error e -> Error e)

    member x.Return(a) =
        Parser(fun tokens consumed_input -> Ok(a, tokens, consumed_input))

    member x.ReturnFrom(parser) = parser
    member x.Delay(f) = f
    member x.Run(f) = f ()

let parser = new ParserBuilder()

let run_parser (Parser parser) tokens = parser tokens false

let fail =
    Parser (fun (tokens: TSqlParserToken list) consumed_input ->
        match tokens with
        | hd :: tl ->
            Error
                { line = hd.Line
                  column = hd.Column
                  expected = "nothing"
                  found = hd.TokenType.ToString()
                  consumed_input = false }
        | [] ->
            Error
                { line = -1
                  column = -1
                  expected = "nothing"
                  found = "end of input"
                  consumed_input = false })

let parse_where expected condition : TSqlParserToken Parser =
    Parser (fun tokens consumed_input ->
        match tokens with
        | [] ->
            Error
                { line = -1
                  column = -1
                  expected = expected
                  found = "end of input"
                  consumed_input = consumed_input }
        | head :: tail ->
            if condition head then
                Ok(head, tail, true)
            else
                Error
                    { line = head.Line
                      column = head.Column
                      expected = expected
                      found = head.TokenType.ToString()
                      consumed_input = consumed_input })

let choice choices =
    Parser (fun tokens consumed_input ->
        let rec iter_choices error =
            function
            | [] -> Error error
            | Parser choice :: choices ->
                (match choice tokens false with
                 | Ok (result, tokens, new_consumed_input) -> Ok(result, tokens, new_consumed_input || consumed_input)
                 | Error new_error ->
                     match new_error.consumed_input with
                     | false ->
                         iter_choices { error with expected = error.expected + ", " + new_error.expected } choices
                     | true -> Error new_error)

        match choices with
        | [] -> failwith "impossible"
        | Parser choice :: choices ->
            match choice tokens false with
            | Ok (result, tokens, new_consumed_input) -> Ok(result, tokens, new_consumed_input || consumed_input)
            | Error error ->
                match error.consumed_input with
                | false -> iter_choices { error with consumed_input = consumed_input } choices
                | true -> Error error)

let ret x =
    Parser(fun tokens consumed_input -> Ok(x, tokens, consumed_input))

let rec parse_many1 p =
    parser {
        let! x = p
        let! xs = parse_many0 p
        return (x :: xs)
    }

and parse_many0 p =
    parser {
        let! x = choice [ parse_many1 p; ret [] ]
        return x
    }

let sep_by1 sep p =
    parser {
        let! x = p

        let! xs =
            parse_many0 (
                parser {
                    let! _ = sep
                    return! p
                }
            )

        return (x :: xs)
    }

let sep_by0 sep p = choice [ sep_by1 sep p; ret [] ]

let parse_token_type_no_whitespace (token_type: TSqlTokenType) =
    parse_where (token_type.ToString()) (fun (token: TSqlParserToken) -> token.TokenType = token_type)

let parse_whitespace =
    parser {
        let! _ = parse_many0 (parse_token_type_no_whitespace TSqlTokenType.WhiteSpace)
        return ()
    }

let parse_token_type token_type =
    parser {
        let! token = parse_token_type_no_whitespace token_type
        do! parse_whitespace
        return token
    }

let parse_keyword token_type =
    parser {
        let! _ = parse_token_type token_type
        return ()
    }

let parse_and_return p value =
    parser {
        do! p
        return value
    }


let optional p =
    choice [ parser {
                 let! x = p
                 return (Some x)
             }
             ret None ]

let parse_token_text (p: TSqlParserToken Parser) =
    parser {
        let! token = p
        return token.Text
    }

module Syntax =
    type SelectDistinct =
        | Distinct
        | All

    type SelectTopUnit =
        | Count
        | Percent

    type Path = Path of string list

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

    and SelectStatement =
        { distinct: SelectDistinct option
          select_list: Expression list
          from: Expression
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

let parse_id =
    parse_token_text (
        choice [ parse_token_type TSqlTokenType.QuotedIdentifier
                 parse_token_type TSqlTokenType.Identifier
                 parse_token_type TSqlTokenType.Convert ]
    )

let parse_select_distinct: Syntax.SelectDistinct Parser =
    choice [ parse_and_return (parse_keyword TSqlTokenType.All) Syntax.All
             parse_and_return (parse_keyword TSqlTokenType.Distinct) Syntax.Distinct ]

let parse_path: Syntax.Path Parser =
    parser {
        let! elements = sep_by1 (parse_keyword TSqlTokenType.Dot) parse_id
        return (Syntax.Path elements)
    }

let rec parse_name_or_asterisk_or_call: Syntax.Expression Parser =
    let rec rest acc : Syntax.Expression Parser =
        choice [ parser {
                     do! parse_keyword TSqlTokenType.Dot
                     return! element acc
                 }
                 parser {
                     do! parse_keyword TSqlTokenType.LeftParenthesis
                     let! parameters = sep_by1 (parse_keyword TSqlTokenType.Comma) parse_expression
                     do! parse_keyword TSqlTokenType.RightParenthesis

                     return
                         Syntax.Call
                             { function_name = Syntax.Path(List.rev acc)
                               parameters = parameters }
                 }
                 ret (Syntax.Name(Syntax.Path(List.rev acc))) ]

    and element acc =
        (choice [ parser {
                      let! element = parse_id
                      return! (rest (element :: acc))
                  }
                  parser {
                      do! parse_keyword TSqlTokenType.Star
                      return Syntax.Asterisk(Syntax.Path(List.rev acc))
                  } ])

    element []


and parse_expression0: Syntax.Expression Parser =
    choice [ parse_name_or_asterisk_or_call
             parser {
                 let! literal =
                     parse_token_text (
                         choice [ parse_token_type TSqlTokenType.Numeric
                                  parse_token_type TSqlTokenType.Integer ]
                     )

                 return Syntax.Literal literal
             } ]

and parse_expression: Syntax.Expression Parser =
    parser {
        let! expression = parse_expression0

        return!
            choice [ parser {
                         do! parse_keyword TSqlTokenType.In
                         do! parse_keyword TSqlTokenType.LeftParenthesis
                         let! subquery = parse_select_statement
                         do! parse_keyword TSqlTokenType.RightParenthesis

                         return
                             Syntax.In
                                 { condition = expression
                                   subquery = subquery }
                     }
                     parser {
                         do! parse_keyword TSqlTokenType.EqualsSign
                         let! other = parse_expression0
                         return Syntax.Equals(expression, other)
                     }
                     parser {
                         let! _ = optional (parse_keyword TSqlTokenType.As)

                         match! optional parse_id with
                         | Some name -> return Syntax.As { expression = expression; name = name }
                         | None -> return expression
                     } ]

    }

and parse_select_statement: Syntax.SelectStatement Parser =
    parser {
        do! parse_keyword TSqlTokenType.Select

        let! distinct = optional parse_select_distinct
        let! select_list = sep_by1 (parse_keyword TSqlTokenType.Comma) parse_expression
        do! parse_keyword TSqlTokenType.From
        let! from = parse_expression

        let! where =
            optional (
                parser {
                    do! parse_keyword TSqlTokenType.Where
                    return! parse_expression
                }
            )

        let! _ = optional (parse_keyword TSqlTokenType.Semicolon)

        return
            { select_list = select_list
              from = from
              where = where
              distinct = distinct
              top = None }
    }

let parse_dml_clause_body: Syntax.DmlClauseBody Parser =
    choice [ parser {
                 let! select_statement = parse_select_statement
                 return (Syntax.Select select_statement)
             } ]

let parse_common_table_expression: Syntax.CommonTableExpression Parser =
    parser {
        let! name = parse_id
        let columns = None
        do! parse_keyword TSqlTokenType.As
        do! parse_keyword TSqlTokenType.LeftParenthesis
        let! query = parse_select_statement
        do! parse_keyword TSqlTokenType.RightParenthesis

        return
            { name = name
              columns = columns
              query = query }
    }

let parse_dml_clause: Syntax.DmlClause Parser =
    parser {
        do! parse_keyword TSqlTokenType.With

        let! locals = parse_many1 parse_common_table_expression

        let! body = parse_dml_clause_body
        return { locals = locals; body = body }
    }

let parse_sql_clause: Syntax.SqlClause Parser =
    choice [ parser {
                 let! dml_clause = parse_dml_clause
                 return (Syntax.Dml dml_clause)
             } ]

let parse_go_statement: Syntax.GoStatement Parser =
    parser {
        do! parse_keyword TSqlTokenType.Go
        let! count = optional (parse_token_text (parse_token_type TSqlTokenType.Numeric))
        return { count = count }
    }

let parse_batch: Syntax.Batch Parser =
    choice [ parser {
                 let! sql_clauses = parse_many1 parse_sql_clause
                 return (Syntax.Sql sql_clauses)
             }
             parser {
                 let! go_statement = parse_go_statement
                 return (Syntax.Go go_statement)
             } ]

let parse_file: Syntax.File Parser =
    parser {
        let! batches = parse_many0 parse_batch
        do! parse_keyword TSqlTokenType.EndOfFile
        return (Syntax.Batches batches)
    }

module Buf =
    type t =
        { mutable indent: int
          mutable at_start_of_line: bool
          builder: System.Text.StringBuilder }

    let string t (s: string) =
        if t.at_start_of_line then
            for _ in 0 .. (t.indent * 4) do
                ignore (t.builder.Append ' ')

        t.at_start_of_line <- false
        ignore (t.builder.Append s)

    let newline t =
        ignore (t.builder.Append '\n')
        t.at_start_of_line <- true

    let indent t = t.indent <- t.indent + 1
    let dedent t = t.indent <- t.indent - 1

    let create (capacity: int) =
        { indent = 0
          at_start_of_line = true
          builder = new System.Text.StringBuilder(capacity) }

    let contents t = t.builder.ToString()

    let iter_sep_by xs sep f =
        match xs with
        | [] -> ()
        | x :: xs ->
            f x

            List.iter
                (fun x ->
                    sep ()
                    f x)
                xs

module Pretty =

    type XGravity =
        | Left
        | Right

    type YGravity =
        | Up
        | Down

    type kind =
        | Empty
        | String of string
        | Horizontal of t * t
        | Vertical of t * t
        | Or_else of t * t

    and t =
        { kind: kind
          width: int
          height: int
          x_gravity: XGravity
          y_gravity: YGravity }

    let empty =
        { kind = Empty
          width = 0
          height = 0
          x_gravity = Left
          y_gravity = Up }

    let string (s: string) =
        assert (not (s.Contains '\n'))

        { kind = String s
          width = s.Length
          height = 1
          x_gravity = Left
          y_gravity = Up }

    let horizontal left right =
        { kind = Horizontal(left, right)
          width = left.width + right.width
          height = max left.height right.height
          x_gravity = Left
          y_gravity = Up }

    let vertical left right =
        { kind = Vertical(left, right)
          width = max left.width right.width
          height = left.height + right.height
          x_gravity = Left
          y_gravity = Up }

    let or_else primary secondary =
        { kind = Or_else(primary, secondary)
          width = primary.width
          height = primary.height
          x_gravity = Left
          y_gravity = Up }

    type t with
        static member (+)(left, right) = horizontal left right
        static member (+)(left, right) = horizontal left (string right)
        static member (+)(left, right) = horizontal (string left) right
        static member (*)(left, right) = vertical left right
        static member (*)(left, right) = vertical left (string right)
        static member (*)(left, right) = vertical (string left) right
        static member op_Implicit(a: string) = string a

    let gravity_left t = { t with x_gravity = Left }
    let gravity_right t = { t with x_gravity = Right }
    let gravity_up t = { t with y_gravity = Up }
    let gravity_down t = { t with y_gravity = Down }

    let to_string t =
        let rec lines t =
            match t.kind with
            | Empty -> []
            | String s -> [ s ]
            | Horizontal (left, right) ->
                let padded_lines x =
                    let padding_length = t.height - x.height
                    let padding_line = String.replicate x.width " "
                    let padding_lines = List.replicate padding_length padding_line

                    match x.y_gravity with
                    | Up -> lines x @ padding_lines
                    | Down -> padding_lines @ lines x

                let left = padded_lines left
                let right = padded_lines right
                assert (List.length left = List.length right)
                List.map (fun (left, right) -> left + right) (List.zip left right)
            | Or_else (primary, _) -> lines primary
            | Vertical (above, below) ->
                let padded_lines x =
                    let padding_length = t.width - x.width
                    let padding_string = String.replicate padding_length " "

                    match x.x_gravity with
                    | Left -> List.map (fun line -> line + padding_string) (lines x)
                    | Right -> List.map (fun line -> padding_string + line) (lines x)

                padded_lines above @ padded_lines below

        String.concat "\n" (lines t)

    let rec shrink t =
        match t.kind with
        | Empty -> None
        | String _ -> None
        | Horizontal (left, right) ->
            match shrink left with
            | Some left -> Some(horizontal left right)
            | None ->
                match shrink right with
                | Some right -> Some(horizontal left right)
                | None -> None
        | Vertical (top, bottom) ->
            match shrink top with
            | Some top -> Some(vertical top bottom)
            | None ->
                match shrink bottom with
                | Some bottom -> Some(vertical top bottom)
                | None -> None
        | Or_else (primary, secondary) ->
            match shrink primary with
            | Some primary -> Some(or_else primary secondary)
            | None -> Some secondary

    let hlist ts = List.fold horizontal empty ts

    let vlist ts = List.fold vertical empty ts

    let sepby f ts =
        match ts with
        | [] -> empty
        | hd :: tl -> List.fold f hd tl

    let hlist_sepby sep = sepby (fun a b -> a + sep + b)
    let vlist_sepby sep = sepby (fun a b -> a * sep * b)

let pp_path (Syntax.Path path) =
    Pretty.hlist_sepby "." (List.map Pretty.string path)

let format_path buf (Syntax.Path path) =
    Buf.iter_sep_by path (fun () -> Buf.string buf ".") (Buf.string buf)

let rec pp_expression expression : Pretty.t =
    match expression with
    | Syntax.Literal literal -> literal
    | Syntax.Name path -> pp_path path
    | Syntax.Asterisk ((Syntax.Path parts) as path) ->
        match parts with
        | [] -> "*"
        | _ :: _ -> pp_path path * ".*"
    | Syntax.Call { function_name = function_name
                    parameters = parameters } ->
        pp_path function_name
        + "("
        + Pretty.hlist_sepby ", " (List.map pp_expression parameters)
        + ")"
    | Syntax.As { name = name; expression = expression } -> pp_expression expression + " AS " + name
    | Syntax.In { condition = condition
                  subquery = subquery } ->
        let line1 = pp_expression condition + " IN"

        let line2 =
            "("
            + pp_select_statement subquery
            + Pretty.gravity_down ")"

        line1 * line2
    | Syntax.Equals (left, right) -> pp_expression left + " = " + pp_expression right

and pp_select_statement
    ({ distinct = distinct
       select_list = select_list
       from = from
       where = where
       top = top }: Syntax.SelectStatement)
    : Pretty.t =
    let distinct =
        match distinct with
        | Some Syntax.All -> Pretty.string "ALL "
        | Some Syntax.Distinct -> Pretty.string "DISTINCT "
        | None -> Pretty.empty

    let line1 = "SELECT " + distinct

    let line2 =
        match select_list with
        | [] -> Pretty.empty
        | hd :: tl ->
            let hd = "  " + pp_expression hd
            let tl = List.map (fun item -> ", " + pp_expression item) tl
            Pretty.vlist (hd :: tl)

    let where =
        match where with
        | Some where -> "WHERE " + pp_expression where
        | None -> Pretty.empty

    line1
    * line2
    * "FROM"
    * ("  " + pp_expression from)
    * where


let rec format_expression buf expression =
    match expression with
    | Syntax.Literal literal -> Buf.string buf literal
    | Syntax.Name path -> format_path buf path
    | Syntax.Asterisk ((Syntax.Path parts) as path) ->
        match parts with
        | [] -> Buf.string buf "*"
        | _ :: _ ->
            format_path buf path
            Buf.string buf ".*"
    | Syntax.Call { function_name = function_name
                    parameters = parameters } ->
        format_path buf function_name
        Buf.string buf "("
        Buf.iter_sep_by parameters (fun () -> Buf.string buf ", ") (format_expression buf)
        Buf.string buf ")"
    | Syntax.As { name = name; expression = expression } ->
        format_expression buf expression
        Buf.string buf " AS "
        Buf.string buf name
    | Syntax.In { condition = condition
                  subquery = subquery } ->
        format_expression buf condition
        Buf.string buf " IN"
        Buf.newline buf
        Buf.indent buf
        Buf.string buf "("
        format_select_statement buf subquery
        Buf.string buf ")"
        Buf.dedent buf
    | Syntax.Equals (left, right) ->
        format_expression buf left
        Buf.string buf " = "
        format_expression buf right

and format_select_statement
    buf
    ({ distinct = distinct
       select_list = select_list
       from = from
       where = where
       top = top }: Syntax.SelectStatement)
    =
    Buf.string buf "SELECT "

    match distinct with
    | Some Syntax.All -> Buf.string buf "ALL "
    | Some Syntax.Distinct -> Buf.string buf "DISTINCT "
    | None -> ()

    Buf.newline buf
    Buf.indent buf

    Buf.iter_sep_by
        select_list
        (fun () ->
            Buf.string buf ","
            Buf.newline buf)
        (format_expression buf)

    Buf.dedent buf
    Buf.newline buf
    Buf.string buf "FROM"
    Buf.newline buf
    Buf.indent buf
    format_expression buf from
    Buf.dedent buf

    match where with
    | Some where ->
        Buf.newline buf
        Buf.string buf "WHERE "
        Buf.newline buf
        Buf.indent buf
        format_expression buf where
        Buf.dedent buf
    | None -> ()

let pp_sql_clause clause =
    match clause with
    | Syntax.Dml { locals = locals; body = body } ->
        let locals =
            match locals with
            | [] -> Pretty.empty
            | _ :: _ ->
                let f
                    ({ name = name
                       columns = columns
                       query = query }: Syntax.CommonTableExpression)
                    =
                    let columns =
                        match columns with
                        | Some columns -> Pretty.hlist_sepby ", " (List.map Pretty.string columns)
                        | None -> Pretty.empty

                    (name + "(" + columns + Pretty.gravity_down ")")
                    * ("AS ("
                       + pp_select_statement query
                       + Pretty.gravity_down ")")

                Pretty.vlist (List.map f locals)

        let body =
            match body with
            | Syntax.Select select_statement -> pp_select_statement select_statement

        "WITH" * ("    " + locals) * body

let format_sql_clause buf clause =
    match clause with
    | Syntax.Dml { locals = locals; body = body } ->
        match locals with
        | [] -> ()
        | _ :: _ ->
            Buf.string buf "WITH"
            Buf.newline buf
            Buf.indent buf

            Buf.iter_sep_by
                locals
                (fun () -> Buf.newline buf)
                (fun { name = name
                       columns = columns
                       query = query } ->
                    Buf.string buf name

                    match columns with
                    | Some columns ->
                        Buf.string buf "("
                        Buf.iter_sep_by columns (fun () -> Buf.string buf ", ") (Buf.string buf)
                        Buf.string buf ")"
                    | None -> ()

                    Buf.newline buf
                    Buf.indent buf
                    Buf.string buf "AS ("
                    Buf.indent buf
                    format_select_statement buf query
                    Buf.string buf ")"
                    Buf.dedent buf
                    Buf.dedent buf)

            Buf.newline buf
            Buf.dedent buf

        match body with
        | Syntax.Select select_statement -> format_select_statement buf select_statement

let pp_batch batch =
    match batch with
    | Syntax.Go { count = count } -> Pretty.string "GO " + count.ToString()
    | Syntax.Sql clauses -> Pretty.vlist (List.map pp_sql_clause clauses)

let format_batch buf batch =
    match batch with
    | Syntax.Go { count = count } ->
        Buf.string buf "GO "
        Buf.string buf (count.ToString())
    | Syntax.Sql clauses -> Buf.iter_sep_by clauses (fun () -> Buf.newline buf) (format_sql_clause buf)

let pp_file (Syntax.Batches batches) =
    Pretty.vlist_sepby "" (List.map pp_batch batches)

let format_file buf (Syntax.Batches batches: Syntax.File) =
    Buf.iter_sep_by
        batches
        (fun () ->
            Buf.newline buf
            Buf.newline buf)
        (format_batch buf)

let main () =
    let filename = get_filename ()
    let tokens = get_file_tokens filename

    match run_parser parse_file tokens with
    | Ok (x, rest_of_tokens, consumed_input) ->
        let result = pp_file x
        let result = Pretty.to_string result
        printfn "%s" result
    | Error e -> log_error (e.ToString())

    printfn ""

let () =
    try
        main ()
    with
    | FatalError -> ()

    match errors with
    | [] -> ()
    | _ :: _ ->
        printfn "Formatter finished with errors:"

        for error in errors do
            printfn "%s" error
