module Pretty

[<Sealed>]
type t =
    static member (+): t * t -> t
    static member (+): string * t -> t
    static member (+): t * string -> t
    static member (*): t * t -> t
    static member (*): string * t -> t
    static member (*): t * string -> t
    static member op_Implicit: string -> t

val string: string -> t
val empty: t
val vlist: t list -> t
val hlist: t list -> t

val hlist_sepby: t -> t list -> t
val vlist_sepby: t -> t list -> t

val to_string: t -> string
