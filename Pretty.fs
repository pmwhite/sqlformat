module Pretty

type kind =
    | VUnit
    | String of string
    | Horizontal of t * t
    | Vertical of t * t
    | Or_else of t * t

and t =
    { kind: kind
      width: int
      height: int
      last_line_width: int }

let vunit =
    { kind = VUnit
      width = 1
      height = 0
      last_line_width = 0 }


let string (s: string) =
    assert (not (s.Contains '\n'))

    { kind = String s
      width = s.Length
      height = 1
      last_line_width = s.Length }

let hunit = string ""

let horizontal left right =
    { kind = Horizontal(left, right)
      width = max left.width (left.last_line_width + right.width)
      height = max left.height right.height
      last_line_width = left.last_line_width + right.last_line_width }

let vertical left right =
    { kind = Vertical(left, right)
      width = max left.width right.width
      height = left.height + right.height
      last_line_width = right.last_line_width }

let or_else primary secondary =
    { kind = Or_else(primary, secondary)
      width = primary.width
      height = primary.height
      last_line_width = primary.last_line_width }

type t with
    static member (+)(left, right) = horizontal left right
    static member (+)(left, right) = horizontal left (string right)
    static member (+)(left, right) = horizontal (string left) right
    static member (*)(left, right) = vertical left right
    static member (*)(left, right) = vertical left (string right)
    static member (*)(left, right) = vertical (string left) right
    static member op_Implicit(a: string) = string a

let rec shrink t =
    match t.kind with
    | VUnit -> None
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

let to_string t width =
    let rec shrink_to_width t =
        if t.width > width then
            match shrink t with
            | Some t -> shrink_to_width t
            | None -> t
        else
            t

    let t = shrink_to_width t

    let rec split_last acc ts =
        match ts with
        | [] -> None
        | [ hd ] -> Some(hd, List.rev acc)
        | hd :: (_ :: _ as tl) -> split_last (hd :: acc) tl

    let rec lines t =
        match t.kind with
        | VUnit -> []
        | String s -> [ s ]
        | Horizontal (left, right) ->
            let left_lines = lines left
            let right_lines = lines right

            match split_last [] left_lines with
            | Some (left_last, left_rest) ->
                match right_lines with
                | [] -> left_lines
                | right_first :: right_rest ->
                    let padding_string = String.replicate left.last_line_width " "
                    let right_rest = List.map (fun line -> padding_string + line) right_rest

                    left_rest
                    @ (left_last + right_first :: right_rest)
            | None -> right_lines
        | Or_else (primary, _) -> lines primary
        | Vertical (above, below) -> lines above @ lines below

    String.concat "\n" (lines t)

let hlist ts = List.fold horizontal hunit ts

let vlist ts = List.fold vertical vunit ts

let sepby f ts =
    match ts with
    | [] -> vunit
    | hd :: tl -> List.fold f hd tl

let hlist_sepby sep ts = sepby (fun a b -> a + sep + b) ts
let vlist_sepby sep ts = sepby (fun a b -> a * sep * b) ts
