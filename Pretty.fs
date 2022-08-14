module Pretty

type kind =
    | VUnit
    | String of string
    | Horizontal of t * t
    | Vertical of t * t
    | Post_or_else of t * t
    | Pre_or_else of t * t

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

let post_or_else primary secondary =
    { kind = Post_or_else(primary, secondary)
      width = primary.width
      height = primary.height
      last_line_width = primary.last_line_width }

let pre_or_else primary secondary =
    { kind = Pre_or_else(primary, secondary)
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

let rec step t max_width =
    match t.kind with
    | VUnit -> None
    | String _ -> None
    | Horizontal (left, right) ->
        match step left max_width with
        | Some left -> Some(horizontal left right)
        | None ->
            match step right (max_width - left.width) with
            | Some right -> Some(horizontal left right)
            | None -> None
    | Vertical (top, bottom) ->
        let step_part t =
            match t.width > max_width with
            | true -> step t max_width
            | false -> None

        match step_part top with
        | Some top ->
            match step_part bottom with
            | Some bottom -> Some(vertical top bottom)
            | None -> Some(vertical top bottom)
        | None ->
            match step_part bottom with
            | Some bottom -> Some(vertical top bottom)
            | None -> None
    | Post_or_else (primary, secondary) ->
        match step primary max_width with
        | Some primary -> Some(post_or_else primary secondary)
        | None -> Some secondary
    | Pre_or_else (_, secondary) -> Some secondary


let to_string t width =


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
        | Post_or_else (primary, _) -> lines primary
        | Pre_or_else (primary, _) -> lines primary
        | Vertical (above, below) -> lines above @ lines below

    let rec shrink_to_width t =
        if t.width > width then
            match step t width with
            | Some t -> shrink_to_width t
            | None -> t
        else
            t

    let t = shrink_to_width t

    String.concat "\n" (lines t)

let hlist ts = List.fold horizontal hunit ts

let vlist ts = List.fold vertical vunit ts

let sepby f ts =
    match ts with
    | [] -> vunit
    | hd :: tl -> List.fold f hd tl

let hlist_sepby sep ts = sepby (fun a b -> a + sep + b) ts
let vlist_sepby sep ts = sepby (fun a b -> a * sep * b) ts
