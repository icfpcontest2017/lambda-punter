(*** lambda punter ***)

(* ignore SIGPIPE signals *)
module Signal = struct
  let sigpipe_handler = Lwt_unix.on_signal Sys.sigpipe (fun _sig -> prerr_endline @@ "SIGPIPE")
end

module Default = struct
  (* Default map:
   *
   * Build this base map

      0--1--2
      | / \ |
      |/   \|
      7     3
      |\   /|
      | \ / |
      6--5--4

    and discrete shadows of it for each punter (no rivers)

      ...........
      .         .
      . 0  1  2 .
      .         .
      .         .
      . 7     3 .
      .         .
      .         .
      . 6  5  4 .
      .         .
      ...........
  *)

  let map =
    "{\"sites\":[{\"id\":0, \"x\": -1.0, \"y\":  1.0}, {\"id\":1, \"x\": 0.0, \"y\":  1.0}, \
                 {\"id\":2, \"x\":  1.0, \"y\":  1.0}, {\"id\":3, \"x\": 1.0, \"y\":  0.0}, \
                 {\"id\":4, \"x\":  1.0, \"y\": -1.0}, {\"id\":5, \"x\": 0.0, \"y\": -1.0}, \
                 {\"id\":6, \"x\": -1.0, \"y\": -1.0}, {\"id\":7, \"x\":-1.0, \"y\":  0.0}],\
      \"rivers\":[{\"source\": 0, \"target\": 1}, {\"source\": 1, \"target\": 2}, \
                  {\"source\": 2, \"target\": 3}, {\"source\": 3, \"target\": 4}, \
                  {\"source\": 4, \"target\": 5}, {\"source\": 5, \"target\": 6}, \
                  {\"source\": 6, \"target\": 7}, {\"source\": 7, \"target\": 0}, \
                  {\"source\": 1, \"target\": 3}, {\"source\": 3, \"target\": 5}, \
                  {\"source\": 5, \"target\": 7}, {\"source\": 7, \"target\": 1}],\
      \"mines\":[1,5]}"
end

module Settings = struct
  let map = ref Default.map

  let map_path = ref None
  let num_punters = ref 2

  let setup_timeout = ref 10.0
  let handshake_timeout = ref 10.0
  let move_timeout = ref 1.0
  let max_timeouts = ref 10

  let address = ref "127.0.0.1"
  let port = ref 9999
  let log_file = ref ""
  let logging_level = ref 1

  let coordinates = ref false

  let eager = ref false

  let offline = ref false
  let punter_file = ref None
  let progs = ref ["eager-futures-splurges-options"; "eager-futures-splurges-options"]

  (* extensions *)
  let splurges = ref false
  let options = ref false
  let futures = ref false

  (* round of the contest *)
  let round = ref None
end

module Loader : sig
end = struct
  open Arg
  open Printf

  let arg_specs = align [
    ("--map",            String (fun s -> Settings.map_path := Some s),           " Read in the graph from a JSON graph file");
    ("--punters",        Set_int Settings.num_punters,                            " Number of punters (default 2)");

    ("--setup-timeout",  Set_float Settings.setup_timeout,                        " Setup timeout (default 10.0)");
    ("--handshake-timeout",  Set_float Settings.handshake_timeout,                " Handshake timeout (default 10.0)");
    ("--move-timeout",   Set_float Settings.move_timeout,                         " Move timeout (default 1.0)");
    ("--max-timeouts",   Set_int Settings.max_timeouts,                           " Maximum number of timeouts (default 10)");

    ("--address",        Set_string Settings.address,                             " IP address");
    ("--port",           Set_int Settings.port,                                   " Port");

    ("--offline",        Set Settings.offline,                                    " Run an offline game");
    ("--punter-file",    String (fun s -> Settings.punter_file := Some s),        " A file containing a list of punters to run in offline mode");

    ("--coordinates",    Set Settings.coordinates,                                " Graphs have coordinate data");

    ("--splurges",       Set Settings.splurges,                                   " Enable splurges");
    ("--options",        Set Settings.options,                                    " Enable options");
    ("--futures",        Set Settings.futures,                                    " Enable futures");

    ("--eager",          Set Settings.eager,                                      " Run as an eager client");

    ("--log-level",      Set_int Settings.logging_level,                          " Logging level (values: 0 to 2)   (default: 1)");
    ("--log",            Set_string Settings.log_file,                            " Log file");

    ("--round",          String (fun s -> Settings.round := Some s),              " Contest round");
  ]

  let _ = Arg.parse arg_specs ignore "Options:"

  let read_file_lines path : string list =
    let ic =
      begin
        try open_in path
        with Sys_error(err) ->
          printf "Error reading file (%s)\n" err;
          exit (-1)
      end in

    let rec read_file_inner ic lines =
      begin
        try
          let line = input_line ic in
          read_file_inner ic (line :: lines)
        with End_of_file ->
          close_in_noerr ic;
          List.rev lines
      end in
    (* Obtain input context, read each line *)
    read_file_inner ic []

  let read_file path = read_file_lines path |> String.concat "\n"

  let _ =
    match !Settings.map_path with
    | Some p ->
      printf "Reading map from file %s\n" p;
      Settings.map := read_file p
    | None -> ()

  let _ =
    if not !Settings.eager && !Settings.offline then
      match !Settings.punter_file with
      | None -> ()
      | Some filename ->
        printf "Reading punter list from file %s\n" filename;
        Settings.progs := read_file_lines filename;
        Settings.num_punters := List.length (!Settings.progs)
end

module Log = struct
  let logger = ref !Lwt_log.default

  let init () =
    let set_level level =
      ignore(if level > 0 then Lwt_log.add_rule "*" Lwt_log.Info);
      ignore(if level > 1 then Lwt_log.add_rule "*" Lwt_log.Debug) in
    set_level !Settings.logging_level;

    let open Lwt in
    if !Settings.log_file <> "" then
      Lwt_log.file !Settings.log_file () >>= fun l ->
      logger := l;
      return ()
    else
      return ()

  (* always displays: log level 0 *)
  let info msg = Lwt_log.notice ~logger:!logger msg
  (* log level 1 *)
  let json msg = Lwt_log.info ~logger:!logger ("JSON:"^(Yojson.Safe.to_string msg))
  (* log level 2 *)
  let debug msg = Lwt_log.debug ~logger:!logger (Lazy.force msg)

  let warning msg = Lwt_log.warning ~logger:!logger msg
  let error msg = Lwt_log.error ~logger:!logger msg
end

module Util = struct
  (* choose one entry from a hash table *)
  let hash_table_choose (type a) (type b) (table : (a, b) Hashtbl.t) =
    let exception Pair of (a * b) in
    try
      Hashtbl.iter (fun k v -> raise @@ Pair (k, v)) table;
      raise Not_found
    with
      Pair (k, v) -> (k, v)
end
open Util

open Graph

(* undirected graphs with integer vertices *)
module Vertex = struct
  type t = int
  let compare = Pervasives.compare
  let equal = (=)
  let hash = Hashtbl.hash
end
module G = Imperative.Graph.Concrete (Vertex)

(* shortest paths *)
module Weight = struct
  type edge = G.edge
  type t = int
  let weight edge = 1
  let compare = Pervasives.compare
  let add = (+)
  let sub = (-)
  let zero = 0
end

module Dijkstra = Path.Dijkstra (G) (Weight)

module Game = struct
  type punter = int
  type site = int
  type river = int * int

  type punter_resources = {credit:int; options:int}

  (* Active credit - a punter that can claim up to credit rivers
     Zombie        - a punter that has been disconnected and can no longer move *)
  type punter_status = Active of punter_resources | Zombie

  type move = Claim of (punter * river)         (* Claim (p, r)        p claims river r *)
            | Pass of punter                    (* Pass p              p passes *)
            | Splurge of (punter * site list)   (* Splurge (p, sites)  p splurges rivers connecting sites *)
            | Option of (punter * river)        (* Option (p, r)       p buys option in river r *)

  (* first punter is the owner; second punter has bought an option *)
  type river_state = (punter * punter option) option

  type futures = (site, site option) Hashtbl.t

  exception IllegalMove of string
  exception IllegalFuture of string

  let string_of_river (i, j) = "(" ^ string_of_int i ^ ", " ^ string_of_int j^ ")"

  let punter_of_move =
    function
    | Claim (p, _)
    | Pass p
    | Option (p, _)
    | Splurge (p, _) -> p

  let punter_prefix p =
    if !Settings.offline then
      "Punter " ^ string_of_int p ^ " ("^List.nth !Settings.progs p^")"
    else
      "Punter "^string_of_int p

  let string_of_claim ?short:(short=false) (p, river) =
    if short then
      "Claim("^string_of_int p^", "^string_of_river river^")"
    else
      punter_prefix p^" claims "^string_of_river river
  let string_of_pass ?short:(short=false) p =
    if short then
      "Pass("^string_of_int p^")"
    else
      punter_prefix p^" passes"
  let string_of_splurge ?short:(short=false) (p, sites) =
    if short then
      "Splurge("^string_of_int p^", [" ^ String.concat ", " (List.map string_of_int sites) ^ "])"
    else
      punter_prefix p^" splurges "^"[" ^ String.concat ", " (List.map string_of_int sites) ^ "]"
  let string_of_option ?short:(short=false) (p, river) =
    if short then
      "Option("^string_of_int p^", "^string_of_river river^")"
    else
      punter_prefix p^" buys option for "^string_of_river river

  let string_of_move ?short:(short=false) =
    function
    | Claim c   -> string_of_claim ~short:short c
    | Pass p    -> string_of_pass ~short:short p
    | Option o  -> string_of_option ~short:short o
    | Splurge s -> string_of_splurge ~short:short s

  type game_settings = {splurges:bool; options:bool; futures:bool}

  let string_of_settings =
    fun {futures; splurges; options} ->
      let opt b s =
        if b then [s]
        else [] in
      "{" ^ (String.concat
               ", "
               (List.concat [opt futures "futures"; opt splurges "splurges"; opt options "options"])) ^ "}"

  (* for use by clients *)
  let apply_settings {splurges=splurges; options=options; futures=futures} =
    (* each setting is only enabled if both the server and the client
    is willing to handle it *)
    begin
      Settings.splurges := !Settings.splurges && splurges;
      Settings.options  := !Settings.options  && options;
      Settings.futures  := !Settings.futures  && futures
    end

  let read_settings () =
    {splurges = !Settings.splurges; options = !Settings.options; futures = !Settings.futures}

  (* game state *)
  type t = {num_punters: int;
            (* static base graph *)
            base: G.t;
            mines: int list;
            coordinates: (site, (float * float)) Hashtbl.t;
            (* dynamic graphs *)
            punter_graphs: G.t array;
            network: (river, river_state) Hashtbl.t;
            free_rivers: (river, unit) Hashtbl.t;
            (* other dynamic game state *)
            status: punter_status array;
            num_moves: int ref;
            (* game settings *)
            settings: game_settings;
            (* futures *)
            futures: futures array}


  let extract_resource p game =
    match game.status.(p) with
    | Active res -> res
    | Zombie     -> {credit = 0; options = 0}

  let update_resource p game res =
    match game.status.(p) with
    | Active _res -> game.status.(p) <- Active res
    | Zombie      -> ()

  let extract_resources game =
    Array.to_list
      (Array.init game.num_punters (fun p -> extract_resource p game))

  let update_resources game resources =
    List.iteri
      (fun p res -> update_resource p game res)
      resources

  let norm_river (source, target) =
    if source < target then
      (source, target)
    else
      (target, source)

  let find_network_river game river =
    Hashtbl.find game.network (norm_river river)

  let add_network_river game river =
    Hashtbl.add game.network (norm_river river)

  let replace_network_river game river =
    Hashtbl.replace game.network (norm_river river)

  let find_free_river game river =
    Hashtbl.find game.free_rivers (norm_river river)

  (* assumes river exists *)
  let is_free_river game river =
    match find_network_river game river with
    | None   -> true
    | Some _ -> false

  let add_free_river game river =
    Hashtbl.add game.free_rivers (norm_river river)

  let replace_free_river game river =
    Hashtbl.replace game.free_rivers (norm_river river)

  let size game = G.nb_edges game.base
  let finished game = !(game.num_moves) = size game
  let first_round game = !(game.num_moves) < game.num_punters

  let valid_punter game p = 0 <= p && p < game.num_punters

  let empty_futures mines =
    let futures = Hashtbl.create (List.length mines) in
    List.iter (fun mine -> Hashtbl.add futures mine None) mines;
    futures

  let check_future game source target =
    if not (List.mem source game.mines) then
      raise @@ IllegalFuture ("Source "^ string_of_int source ^ " is not a mine");
    if List.mem target game.mines then
      raise @@ IllegalFuture ("Target "^ string_of_int source ^ " is a mine");
    if not (G.mem_vertex game.base target) then
      raise @@ IllegalFuture ("Target "^ string_of_int source ^ " is not a site")

  let check_river_available game river =
    match find_network_river game river with
    | None                -> ()
    | Some (p, _)         -> raise @@ IllegalMove ("Illegal claim: punter " ^ string_of_int p ^
                                                     " already owns " ^ string_of_river river)
    | exception Not_found -> raise @@ IllegalMove ("Illegal claim: no river " ^ string_of_river river)

  let check_river_owner p game river =
    match find_network_river game river with
    | None                      -> raise @@ IllegalMove ("Illegal option: nobody owns river " ^ string_of_river river)
    | Some (q, None) when p = q -> raise @@ IllegalMove ("Illegal option: punter " ^ string_of_int p ^
                                                           " already owns " ^ string_of_river river);
    | Some (q, None)            -> q
    | Some (_, Some p)          -> raise @@ IllegalMove ("Illegal option: punter " ^ string_of_int p ^
                                                           " already has an option on " ^ string_of_river river)
    | exception Not_found       -> raise @@ IllegalMove ("Illegal option: no river " ^ string_of_river river)

  let check_splurge p game sites options : move list =
    let grabbed = Hashtbl.create ((List.length sites)-1) in
    let rec splurge s sites options =
      match sites with
      | [] -> []
      | t :: sites ->
         let river = norm_river (s, t) in
         if Hashtbl.mem grabbed river then
           raise @@ IllegalMove (punter_prefix p^
                                   " attempting to splurge the same river twice: " ^
                                     string_of_river river);
         Hashtbl.add grabbed river ();
         begin
           try
             check_river_available game river;
             Claim (p, (s, t)) :: splurge t sites options
           with
           | IllegalMove msg ->
              begin
                if options > 0 then
                  let _q = check_river_owner p game river in
                  Option (p, (s, t)) :: splurge t sites (options-1)
                else
                  raise (IllegalMove msg)
              end
         end in
    match sites with
    | [] -> raise @@ IllegalMove (punter_prefix p^
                                    " attempting an malformed splurge: " ^
                                      "[]")
    | [s] -> raise @@ IllegalMove (punter_prefix p^
                                     " attempting an malformed splurge: " ^
                                       "[" ^ string_of_int s ^ "]")
    | s :: sites ->
       begin
         try
           splurge s sites options
         with
         | IllegalMove msg ->
            raise @@ IllegalMove (punter_prefix p^
                                    " attempting an illegal splurge\n" ^ msg)
       end

  let apply_claim game (p, river) =
    let res = extract_resource p game in
    let (i, j) as river = norm_river river in
    (* prerr_endline ("Applying claim: ("^string_of_int p^", "^string_of_river river^")"); *)
    Hashtbl.replace game.network river (Some (p, None));
    Hashtbl.remove game.free_rivers river;
    G.add_edge game.punter_graphs.(p) i j;
    update_resource p game {res with credit = res.credit - 1}

  let apply_option game p q river =
    let res = extract_resource p game in
    let (i, j) as river = norm_river river in
    (* prerr_endline ("Applying option: ("^string_of_int p^", "^string_of_river river^")"); *)
    Hashtbl.replace game.network river (Some (q, Some p));
    G.add_edge game.punter_graphs.(p) i j;
    update_resource p game {credit = res.credit - 1; options = res.options - 1}

  (* add one credit *)
  let incr_credit p game =
    let res = extract_resource p game in
    update_resource p game {res with credit = res.credit + 1}

  (* pass punter p *)
  let pass game p = ()

  (* claim river (i, j) for punter p *)
  let claim game (p, (i, j)) =
    match game.status.(p) with
    | Zombie     -> ()
    | Active res ->
      if not (valid_punter game p) then
        raise @@ IllegalMove (punter_prefix p ^ " is invalid");
      let river = if i <= j then (i, j) else (j, i) in
      check_river_available game river;
      apply_claim game (p, river)

  (* option on river (i, j) for punter p *)
  let option game (p, (i, j)) =
    match game.status.(p) with
    | Zombie     -> ()
    | Active res ->
       if not game.settings.options then
         raise @@ IllegalMove ("Options not enabled");
       if not (valid_punter game p) then
         raise @@ IllegalMove (punter_prefix p^" is invalid");
       let options = res.options in
       if options <= 0 then
         raise @@ IllegalMove (punter_prefix p^" has bought all available options");
       let river = if i <= j then (i, j) else (j, i) in
       let q = check_river_owner p game river in
       apply_option game p q river

  (* splurge the list of rivers given by sites for p *)
  let splurge game p sites =
    match game.status.(p) with
    | Zombie     -> ()
    | Active res ->
      let max = res.credit in
      if not game.settings.splurges then
        raise @@ IllegalMove ("Splurges not enabled");
      if not (valid_punter game p) then
        raise @@ IllegalMove (punter_prefix p^" is invalid");
      let n = (List.length sites) - 1 in
      if n > max then
        raise @@ IllegalMove (punter_prefix p ^
                              " attempted to claim " ^ string_of_int n ^
                              " rivers (max: "^ string_of_int max ^ ")")
      else
        let moves = check_splurge p game sites (res.options) in
        List.iter
          (function
            | Claim c ->
              apply_claim game c
            | Option (p, river) ->
              let q = check_river_owner p game river in
              apply_option game p q river
            | Pass _
            | Splurge _ -> assert false)
          moves

  let make_move game move =
    incr_credit (punter_of_move move) game;
    match move with
    | Claim c            -> claim game c
    | Pass p             -> pass game p
    | Splurge (p, sites) -> splurge game p sites
    | Option c           -> option game c
end
open Game

module Json = struct
  type json = Yojson.Safe.json

  open Yojson.Safe.Util
  exception MalformedMap of string * string
  exception MalformedMode of string * string
  exception MalformedMove of string * string
  exception MalformedState of string * string
  exception MalformedServerMessage of json

  let json_to_string json = Yojson.Safe.to_string json
  let from_string s = Yojson.Safe.from_string s

  let num_to_float =
    function
    | `Int i -> float_of_int i
    | `Float f -> f
    | _ -> failwith "Not a number"

  (* join two association lists *)
  let join xs ys =
    match xs, ys with
    | `Assoc xs, `Assoc ys -> `Assoc (xs @ ys)
    | _ -> assert false

  (* Maps *)
  (* input a map as an initial game state *)
  let input_map game_settings num_punters map =
    let base = G.create () in
    let coordinates = Hashtbl.create 8192 in

    let punter_graphs = Array.init num_punters (fun _p -> G.create ()) in
    let network = Hashtbl.create 8192 in
    let free_rivers = Hashtbl.create 8192 in

    let add_edge i j =
      let i, j = if i < j then i, j else j, i in
      G.add_edge base i j;
      Hashtbl.replace network (i, j) None;
      Hashtbl.replace free_rivers (i, j) () in
    try
      let sites = map |> member "sites" |> to_list in
      List.iter
        (fun site ->
           let i = site |> member "id" |> to_int in
           G.add_vertex base (G.V.create i);
           for j = 0 to num_punters-1 do
             G.add_vertex punter_graphs.(j) (G.V.create i)
           done;
           (* coordinates *)
           if !(Settings.coordinates) then
             let x = num_to_float (site |> member "x") in
             let y = num_to_float (site |> member "y") in
             Hashtbl.add coordinates i (x, y))
        sites;

      let rivers = map |> member "rivers" |> to_list in
      List.iter
        (fun edge ->
           let i = edge |> member "source" |> to_int in
           let j = edge |> member "target" |> to_int in
           add_edge i j)
        rivers;

      (* if there are mines then read them in *)
      let mines =
        match map |> member "mines" with
        | `Null -> raise @@ MalformedMap(json_to_string map, "Map has no mines")
        | json_mines ->
          List.map
            (fun mine -> mine |> to_int)
            (json_mines |> to_list) in

      let options =
        if !Settings.options then
          List.length mines
        else
          0 in

      let status =
        Array.init
          num_punters
          (fun _p -> Active {credit = 0; options = options}) in

      let futures =
        Array.init
          num_punters
          (fun _ -> Game.empty_futures mines) in

      { num_punters = num_punters;

        base = base;
        mines = mines;
        coordinates = coordinates;

        punter_graphs = punter_graphs;
        network = network;
        free_rivers = free_rivers;

        status = status;
        num_moves = ref 0;

        settings = game_settings;
        futures = futures }
    with
    | Yojson.Json_error(msg) -> raise @@ MalformedMap(json_to_string map, msg)
    | _                      -> raise @@ MalformedMap(json_to_string map, "")
  let output_map game =
    let g = game.base in
    let sites =
      if !(Settings.coordinates) then
        G.fold_vertex
          (fun i sites ->
             let (x, y) = Hashtbl.find game.coordinates i in
             `Assoc [("id", `Int i); ("x", `Float x); ("y", `Float y)] :: sites)
          g []
      else
        G.fold_vertex (fun i sites -> `Assoc [("id", `Int i)] :: sites) game.base [] in
    let rivers = G.fold_edges (fun j i rivers -> `Assoc [("source", `Int i); ("target", `Int j)] :: rivers) g [] in
    `Assoc [("sites",  `List sites);
            ("rivers", `List rivers);
            ("mines",  `List (List.map (fun i -> `Int i) game.mines))]

  (* Settings *)
  let input_settings s =
    let get_bool_setting s name =
      match s |> member name with
      | `Bool b -> b
      | _       -> false in

    match s |> member "settings" with
    | `Null -> {splurges=false; options=false; futures=false}
    | s ->
      let splurges = get_bool_setting s "splurges" in
      let options  = get_bool_setting s "options" in
      let futures  = get_bool_setting s "futures" in
      {splurges=splurges; options=options; futures=futures}
  let output_settings settings =
    let add_setting b name settings =
      if b then
        (name, `Bool true) :: settings
      else
        settings in
    let settings =
      add_setting settings.splurges "splurges"
        (add_setting settings.options "options"
           (add_setting settings.futures "futures" [])) in
    `Assoc [("settings", `Assoc settings)]

  (* Game state *)
  let input_game_state s =
    let p = s |> member "punter" |> to_int in
    let n = s |> member "punters" |> to_int in
    let map = s |> member "map" in
    let settings = input_settings s in
    (p, n, map, settings)
  let output_game_state p n map (settings : Game.game_settings) =
    if not settings.futures && not settings.splurges && not settings.options then
      (* if there are no extensions then omit the settings field*)
      (`Assoc [("punter", `Int p);
               ("punters", `Int n);
               ("map", map)])
    else
      join
        (`Assoc [("punter", `Int p);
                 ("punters", `Int n);
                 ("map", map)])
        (output_settings settings)

  (* Splurging goals *)
  let input_goal goal =
    match goal |> member "goal" |> to_string with
    | "splurging" -> `Splurging
    | "saving"    -> `Saving
    | _           -> assert false
  let output_goal =
    function
    | `Splurging -> `Assoc [("goal", `String "splurging")]
    | `Saving    -> `Assoc [("goal", `String "saving")]

  (* State *)
  let input_state m =
    try
      m |> member "state"
    with
    | Yojson.Json_error(msg) -> raise @@ MalformedState(json_to_string m, msg)
    | _                      -> raise @@ MalformedState(json_to_string m, "")
  let output_state state = `Assoc [("state", state)]

  let with_state json state =
    match json with
    | `Assoc xs ->
       `Assoc (("state", state) :: xs)
    | _ -> assert false

  let without_state json =
    match json with
    | `Assoc xs ->
      `Assoc (List.filter (fun (label, value) -> label <> "state") xs)
    | _ -> json

  (* Claim *)
  let rec input_claim claim =
    let claim = claim |> member "claim" in
    let p = claim |> member "punter" |> to_int in
    let i = claim |> member "source" |> to_int in
    let j = claim |> member "target" |> to_int in
    (p, (i, j))
  let output_claim (p, (i, j)) =
    `Assoc [("punter", `Int p);
            ("source", `Int i);
            ("target", `Int j)]

  (* Move *)
  let input_move m =
    try
      match m |> member "claim" with
      | `Null ->
         begin
           match m |> member "pass" with
           | `Null ->
              begin
                match m |> member "splurge" with
                | `Null ->
                   begin
                     match m |> member "option" with
                     | `Null ->
                        raise @@ MalformedMove (json_to_string m, "Unknown kind of move")
                     | m ->
                        (* if running as a client then be prepared to
                        accept any kind of move *)
                        if !Settings.eager || !Settings.options then
                          let p = m |> member "punter" |> to_int in
                          let i = m |> member "source" |> to_int in
                          let j = m |> member "target" |> to_int in
                          Option (p, (i, j))
                        else
                          raise @@ MalformedMove (json_to_string m, "Splurges disabled")
                   end
                | m ->
                   (* if running as a client then be prepared to
                   accept any kind of move *)
                   if !Settings.eager || !Settings.splurges then
                     let p = m |> member "punter" |> to_int in
                     let claims = m |> member "route" |> to_list in
                     Splurge (p, List.map to_int claims)
                   else
                     raise @@ MalformedMove (json_to_string m, "Splurges disabled")
              end
           | m ->
              let p = m |> member "punter" |> to_int in
              Pass p
         end
      | m ->
         let p = m |> member "punter" |> to_int in
         let i = m |> member "source" |> to_int in
         let j = m |> member "target" |> to_int in
         Claim (p, (i, j))
    with
    | Yojson.Json_error(msg)   -> raise @@ MalformedMove(json_to_string m, msg)
    | MalformedMove(_, _) as e -> raise e
    | _                        -> raise @@ MalformedMove(json_to_string m, "")
  let output_move move =
    match move with
    | Claim claim ->
       `Assoc [("claim", output_claim claim)]
    | Pass p ->
       (`Assoc [("pass",
                 `Assoc [("punter", `Int p)])])
    | Splurge (p, sites) ->
       `Assoc [("splurge",
                `Assoc [("punter", `Int p);
                        ("route",
                         `List (List.map (fun s -> `Int s) sites))])]
    | Option claim ->
       `Assoc [("option", output_claim claim)]

  (* Moves *)
  let input_moves ms =
    let moves = ms |> member "moves" |> to_list in
    List.map input_move moves
  let output_moves moves =
    `Assoc [("moves", `List (List.map output_move moves))]

  (* Timeout *)

  (* as specified in the protocol *)
  let output_timeout t =
    `Assoc [("timeout", `Float t)]

  (* for logging *)
  let output_punter_timeout p =
    `Assoc [("timeout", `Assoc [("punter", `Int p)])]

  (* Move request *)
  let input_move_request r =
    input_moves (r |> member "move")
  let output_move_request moves =
    `Assoc [("move", output_moves moves)]

  (* Score *)
  let input_score s =
    let p = s |> member "punter" |> to_int in
    let score = s |> member "score" |> to_int in
    (p, score)
  let output_score p score =
    if !Settings.offline then
      `Assoc [("punter", `Int p); ("score", `Int score); ("team", `String (List.nth !Settings.progs p))]
    else
      `Assoc [("punter", `Int p); ("score", `Int score)]

  (* Scores *)
  let input_scores s =
    let ss = s |> member "scores" |> to_list in
    List.map input_score ss
  let output_scores scores =
    let scores = Array.to_list scores in
    `Assoc [("scores", `List (List.mapi output_score scores))]

  (* Future scores *)
  let output_future_scores future_scores =
    let future_scores = Array.to_list future_scores in
    `Assoc [("futures",
             `List (List.mapi
                      (fun p xs ->
                         `Assoc [("punter", `Int p);
                                 ("scores", `List (List.map (fun (m, s) -> `Assoc [("mine", `Int m); ("score", `Int s)]) xs))])
                      future_scores))]

  (* Stop *)
  let input_stop s =
    let s = s |> member "stop" in
    let moves = input_moves s in
    let scores = input_scores s in
    (moves, scores)
  let output_stop moves scores =
    `Assoc [("stop", join (output_moves (Array.to_list moves)) (output_scores scores))]

  (* Move request or stop *)
  let input_move_request_or_stop x =
    match x |> member "move" with
    | `Null -> `Stop (input_stop x)
    | _     -> `Move (input_move_request x)

  (* Server message *)
  let input_server_message_type msg =
    match msg |> member "punter" with
    | `Null ->
       begin
         match msg |> member "move" with
         | `Null ->
            begin
              match msg |> member "stop" with
              | `Null ->
                 raise @@ MalformedServerMessage((msg : json))
              | _ -> `Stop
            end
         | _ -> `Move
       end
    | _ -> `Setup

  (* Create a game from a map *)
  let create_game game_settings num_punters map =
    try
      input_map game_settings num_punters map
    with
      MalformedMap(json, msg) ->
      prerr_endline ("Fatal error: malformed map (" ^ msg ^ ")");
      prerr_endline ("json: "^json);
      exit (-1)

  (* Me *)
  let input_me me =
    me |> member "me" |> to_string
  let output_me name =
    `Assoc [("me", `String name)]

  (* You *)
  let input_you you =
    you |> member "you" |> to_string
  let output_you name =
    `Assoc [("you", `String name)]

  (* Ready *)
  let input_ready ready =
    ready |> member "ready" |> to_int
  let output_ready p =
    `Assoc [("ready", `Int p)]

  (* Future *)
  let input_future future =
    let source = future |> member "source" |> to_int in
    let target = future |> member "target" |> to_int in
    (source, target)
  let output_future (source, target) =
    `Assoc [("source", `Int source); ("target", `Int target)]

  (* Futures *)
  let input_futures futures =
    let futures =
      match futures |> member "futures" with
      | `Null   -> []
      | futures -> futures |> to_list in
    List.map input_future futures
  let output_futures futures =
    `Assoc [("futures", `List (List.map output_future futures))]

  (* Punter status *)
  let output_punter_status p =
    function
    | Active {credit; options} ->
      let credit = if !Settings.splurges then [("credit", `Int credit)] else [] in
      let options = if !Settings.options then [("options", `Int options)] else [] in
      `Assoc ([("punter", `Int p); ("active", `Bool true)] @ credit @ options)
    | Zombie ->
      `Assoc [("punter", `Int p); ("active", `Bool false)]
  let output_punter_statuses status =
    `Assoc [("statuses", `List (List.mapi output_punter_status (Array.to_list status)))]

  (* Zombie *)
  let output_zombie p =
    `Assoc [("zombie", `Assoc [("punter", `Int p)])]

  (* Gameplay *)
  let output_gameplay =
    function
    | `Start ->
      `Assoc [("gameplay", `String "start")]
    | `Stop ->
        `Assoc [("gameplay", `String "stop")]
end
type json = Json.json

module Score : sig
  type data = (int * int Dijkstra.H.t) list
  val calc_total : data -> Game.futures -> G.t -> int
  val calc_scores : data -> Game.futures array -> G.t array -> (int * (int * int) list) array
  val report_scores : (int * (int * int) list) array -> unit Lwt.t
end = struct
  type data = (int * int Dijkstra.H.t) list

  (* Scoring:

     1) Compute all pairs of connected sites in the punter graph.
     (Currently we do this at the end by computing the strongly connected
     components. We could instead compute the connected pairs
     incrementally, but it isn't clear whether doing so would be more
     efficient.)

     2) To score a punter: take the sum of all the scores for all mines
     in the punter graph. The score for a mine M is the sum of the
     M-scores for all sites in the punter graph. The M-score for site X
     is:

        - 0, if X is not reachable from M in the punter graph

        - r*r, if X is reachable from M in the punter graph, where r
     is the length of the shortest path in the base graph between M
     and X

     Options allow rivers owned by other punters to appear in P's punter
     graph.

     Futures:

     Score r*r*r for success or -(r*r*r) for failure (where r is as above)
  *)

  module C = Components.Undirected (G)

  (* function to apply to individual path lengths *)
  let path_score length = length * length

  let future_path_score length = length * length * length

  let calc_mine (i, paths) futures f =
    let future_score =
      match Hashtbl.find futures i with
      | None -> 0
      | Some j ->
        let x = future_path_score (Dijkstra.H.find paths j) in
        if f i = f j then x
        else -x in
    let route_score =
      Dijkstra.H.fold
        (fun j length total ->
           if i <> j && f i = f j then
             total + path_score length
           else
             total)
        paths 0 in
    (future_score + route_score, future_score)

  let calc_mines mines futures map =
    let _n, f = C.components map in
    List.fold_left
      (fun (total, future_scores) mine ->
         let t, fs = calc_mine mine futures f in
         (total + t, (fst mine, fs) :: future_scores))
      (0, [])
      mines

  let calc_total data futures map = fst (calc_mines data futures map)

  let calc_scores data futures maps =
    Array.mapi
      (fun p map ->
         calc_mines data futures.(p) map)
      maps

  let report_scores scores =
    let open Lwt in
    let total_scores = Array.map fst scores in

    (* sort the futures before logging *)
    let future_scores = Array.map (fun (_, xs) -> List.sort (fun (m, _) (m', _) -> m - m') xs) scores in

    Log.json (Json.output_scores total_scores) >>= fun () ->
    begin
      if !Settings.futures then
        Log.json (Json.output_future_scores future_scores)
      else
        return ()
    end >>= fun () ->
    let rec show_scores p =
      if p = Array.length scores then
        return ()
      else
        (* total score *)
        Log.info (punter_prefix p^" score: " ^ string_of_int (total_scores.(p))) >>= fun () ->
        begin
          if !Settings.futures then
            (* breakdown of futures score *)
            List.fold_left
              (fun m (mine, score) ->
                m >>= fun () ->
                Log.info ("Mine " ^ string_of_int mine ^ " score: "^string_of_int score))
              (return ())
              (future_scores.(p))
          else
            return ()
        end >>= fun () ->
        show_scores (p+1) in
    show_scores 0
end

module Repl = struct
  let rec go game scoring_data =
    let open Lwt in
    let open Lwt_io in
    read_line stdin >>=
    fun msg -> match msg with
    | "score" ->
      let scores = Score.calc_scores scoring_data game.futures game.punter_graphs in
      Score.report_scores scores >>= fun () ->
      go game scoring_data
    | "quit" ->
      return ()
    | _ -> go game scoring_data
end

(* each string is prefixed by its size: <size>:<string> *)
module BoundedIO = struct
  exception MessageTooBig of int
  exception BadCharacter of char
  exception SigPipe

  open Lwt
  let send oc s =
    (* HACK: two flushed writes are apparently sufficient to guarantee
       that an EPIPE exception is raised if the output channel has
       been closed:

         https://github.com/mirage/ocaml-cohttp/issues/57
    *)
    Lwt_io.write oc (string_of_int @@ (String.length s)+1) >>= fun () ->
    Lwt_io.write_char oc ':' >>= fun () ->
    Lwt_io.flush oc >>= fun () ->
    Lwt_io.write oc s >>= fun () ->
    Lwt_io.write_char oc '\n' >>= fun () ->
    Lwt_io.flush oc
  let recv ic =
    let size_buf = Buffer.create 9 in
    let rec read_size ignoring_space count =
      (* don't allow more than 9 digits *)
      if count > 9 then
        fail (MessageTooBig count)
      else
        Lwt_io.read_char ic >>= function
        | ' ' | '\012' |'\n' |'\r' |'\t' when ignoring_space -> read_size true count
        | c when '0' <= c && c <= '9' ->
          Buffer.add_char size_buf c;
          read_size false (count+1)
        | ':' ->
          return (int_of_string @@ Buffer.contents size_buf)
        | c ->
          fail (BadCharacter c) in
    read_size true 0 >>= fun size ->
    begin
      if size > 99999999 then
        Log.warning ("Big message size: " ^ string_of_int size)
      else
        return ()
    end >>= fun () ->
    let data_buf = Bytes.create size in
    Lwt_io.read_into_exactly ic data_buf 0 size >>= fun () ->
    return (Bytes.to_string data_buf)
end

(* DEPRECATED *)
(* each string is terminated by a newline *)
module LineIO = struct
  open Lwt

  (* The following code is based on that for Lwt_io.read_line
  augmented with a maximum number of characters to allow. *)
  (* The handling of carriage returns seems rather useless to me. *)
  let read_line_max ic n =
    let buf = Buffer.create 128 in
    let rec loop cr_read count =
      if count = n then
        fail End_of_file
      else
        Lwt.try_bind (fun _ -> Lwt_io.read_char ic)
                     (function
                      | '\n' ->
                         Lwt.return(Buffer.contents buf)
                      | '\r' ->
                         if cr_read then Buffer.add_char buf '\r';
                         loop true (count+1)
                      | ch ->
                         if cr_read then Buffer.add_char buf '\r';
                         Buffer.add_char buf ch;
                         loop false (count+1))
                     (function
                      | End_of_file ->
                         if cr_read then Buffer.add_char buf '\r';
                         Lwt.return(Buffer.contents buf)
                      | exn ->
                         Lwt.fail exn) in
    Lwt_io.read_char ic >>= function
    | '\r' -> loop true 1
    | '\n' -> Lwt.return ""
    | ch -> Buffer.add_char buf ch; loop false 1

  let max_chars = 10000000

  let send oc s = Lwt_io.write_line oc s
  let recv ic = read_line_max ic max_chars
end


module EagerGameplay = struct
  (* Futures *)
  let make_eager_futures p game =
    List.iter
      (fun mine ->
        let neighbours = G.succ_e game.base mine in
        match neighbours with
        | [] -> ()
        | (site :: _) ->
           Hashtbl.add game.futures.(p) mine (Some (snd site)))
      game.mines

  let setup_futures p game =
    if !Settings.futures then
      (* futures enabled on both client and server *)
      begin
        make_eager_futures p game;
        Hashtbl.fold
          (fun mine site futures ->
             match site with
             | None -> futures
             | Some site -> (mine, site) :: futures)
           game.futures.(p)
           []
      end
    else
      []

  (* Splurges *)
  let saving_size game =
    (Game.size game - !(game.num_moves)) / (2 * game.num_punters)

  let grab_sites p game credit options =
    let grabbed = Hashtbl.create credit in
    let rec grab source sites credit options =
      let sites' = source :: sites in
      if credit = 0 then
        (sites', {credit; options})
      else
        let rec choose_river =
          function
          | [] -> (sites', {credit; options})
          | (_source, target) as river :: rivers ->
             (* prerr_endline ("chasing river: "^string_of_river (source, target)); *)
            let river = Game.norm_river river in
            if not (Hashtbl.mem grabbed river) then
              let river_state = Game.find_network_river game river in
              begin
                match river_state with
                | None ->
                  Hashtbl.add grabbed river ();
                  grab target sites' (credit-1) options
                | Some (q, None) when options > 0 && p <> q ->
                  Hashtbl.add grabbed river ();
                  grab target sites' (credit-1) (options-1)
                | _ ->
                  choose_river rivers
              end
            else
              choose_river rivers
        in
        choose_river (G.succ_e game.base source) in
    let ((source, target) as river, ()) = hash_table_choose game.free_rivers in
    (* prerr_endline ("credit: "^string_of_int credit); *)
    (* prerr_endline ("chose river: "^string_of_river river); *)
    Hashtbl.add grabbed river ();
    let (rev_sites, resources) = grab target [source] (credit-1) options in
    List.rev rev_sites, resources

  let rec pass p = Pass p

  let splurge p game {credit; options} =
    let sites, ({credit=credit'; options=options'} as res) = grab_sites p game credit options in
    (* prerr_endline ("Splurging: "^string_of_splurge sites); *)
    let move =
    match sites with
    | [s1; s2] ->
      if options' = options then
        Claim (p, (s1, s2))
      else
        Option (p, (s1, s2))
    | _ ->
      Splurge (p, sites) in
    move, res

  let select_move p game ({credit; options}, goal) =
    let grab_free_river () =
      let (river, ()) = hash_table_choose game.free_rivers in
      (* prerr_endline ("Claiming river: "^string_of_river river); *)
      (Claim (p, river), {credit; options}), goal in
    let grab_river () =
      if !Settings.options && options > 0 then
        let (river, river_state) = hash_table_choose game.network in
        match river_state with
        | Some (q, None) when p <> q ->
          (* prerr_endline ("Option on river: "^string_of_river river); *)
          (Option (p, river), {credit; options = options-1}), goal
        | _ ->
          grab_free_river ()
      else
        grab_free_river () in

    if !Settings.splurges then
      (* splurges enabled on both client and server *)
      match goal with
      | `Saving ->
        let n = saving_size game in
        if credit >= n then
          (splurge p game {credit; options}), `Saving
        else
          (pass p, {credit; options}), `Saving
      | `Splurging ->
        if credit > 0 then
          (splurge p game {credit; options}), `Splurging
        else
          begin
            (* prerr_endline ("Game size: "^string_of_int (Game.size game)); *)
            (* prerr_endline ("num of punters: "^string_of_int game.num_punters); *)
            (* prerr_endline ("number of moves: "^string_of_int !(game.num_moves)); *)
            (* prerr_endline ("Saving size: "^string_of_int (saving_size game)); *)
            (pass p, {credit; options}), `Saving
          end
    else
      grab_river ()

  (* Resources *)
  let setup_resources p game settings =
    (* number of options if they're enabled *)
    if settings.options then
      begin
        (* prerr_endline ("options enabled on the server"); *)
        Array.iteri
          (fun q ->
             function
             | Active res ->
               assert (res.credit = 0);
               (* players before me in the turn order get 1 credit to
                  account for the initial pass I'm not replaying *)
               let credit = if q < p then 1 else 0 in
               (* other players may have options even if I don't *)
               let options = if p = q && not !Settings.options then 0 else List.length game.mines in
               game.status.(q) <- Active {credit; options}
             | Zombie -> ())
          game.status
      end
    else
      ()

  (* Ensure p's moves take place first *)
  let sort_moves p num_punters moves =
    (* sort moves so that p's move takes place first *)
    let sort moves =
      List.sort
        (fun m1 m2 ->
          ((Game.punter_of_move m1 - p + num_punters) mod num_punters) -
            ((Game.punter_of_move m2 - p + num_punters) mod num_punters))
        moves in
    (* split the list of moves up into sorted chunks of length
    num_punter *)
    let rec split i chunk chunks moves =
      if i = num_punters then
        split 0 [] (chunk::chunks) moves
      else
        match moves with
        | [] ->
           assert (chunk = []);
           chunks
        | move::moves ->
           split (i+1) (move :: chunk) chunks moves in
    if List.length moves = num_punters then
      sort moves
    else
      (* More than num_punter moves => we're in offline mode and some
      timeouts have occurred *)
      (* The implementation relies on the moves received from the
      server being ordered by turn (but not in any particular order
      within a turn). *)
      List.concat (split 0 [] [] moves)
end

(* Implementation of an eager client that always picks the first free
river it finds *)
module OnlineEagerClient = struct
  open Lwt

  type chan = {fd: Lwt_unix.file_descr; ic: Lwt_io.input_channel; oc: Lwt_io.output_channel}

  let socket_domain () = Lwt_unix.PF_INET
  let address () = Lwt_unix.ADDR_INET(Unix.inet_addr_of_string (!Settings.address), !Settings.port)

  let send c json =
    let s = Json.json_to_string json in
    Log.debug (lazy ("Client sending: "^s)) >>= fun () ->
    BoundedIO.send c.oc s
  let recv c =
    BoundedIO.recv c.ic >>= fun s ->
    Log.debug (lazy ("Client received: "^s)) >>= fun () ->
    return (Json.from_string s)

  let rec play_game (p : punter) c game goal =
    recv c >>= fun next ->
    match Json.input_move_request_or_stop next with
    | `Move moves ->
       assert (List.length moves = game.num_punters);
       let moves = EagerGameplay.sort_moves p game.num_punters moves in

       (* apply moves from last turn *)
       List.iter (fun move -> Game.make_move game move; incr game.num_moves) moves;
       let {credit; options} = Game.extract_resource p game in

       Log.debug (lazy ("Number of moves: "^string_of_int !(game.num_moves))) >>= fun () ->
       Log.debug (lazy ("Credit: "^string_of_int credit)) >>= fun () ->
       Log.debug (lazy ("Options: "^string_of_int options)) >>= fun () ->

       let ((move, _resources), goal) = EagerGameplay.select_move p game ({credit; options}, goal) in

       send c (Json.output_move move) >>= fun () ->
       play_game p c game goal
    | `Stop (moves, scores) ->
       List.iter (Game.make_move game) moves;
       List.fold_right
         (fun (p, score) m ->
            Log.info (punter_prefix p ^ " score: "^string_of_int score) >>= fun () -> m)
         scores
         (return ())

  let setup_game fd =
    let c = {fd=fd; ic=Lwt_io.of_fd Lwt_io.Input fd; oc=Lwt_io.of_fd Lwt_io.Output fd} in

    (* handshake *)
    send c (Json.output_me "eager punter") >>= fun () ->
    recv c >>= fun you ->
    ignore(Json.input_you you);

    (* setup *)
    recv c >>= fun init ->
    let (p, n, map, settings) = Json.input_game_state init in
    Game.apply_settings settings;
    Log.info (punter_prefix p ^ "; Number of punters: " ^ string_of_int n) >>= fun () ->
    let game = Json.create_game settings n map in
    let futures = EagerGameplay.setup_futures p game in
    send c (Json.join (Json.output_ready p) (Json.output_futures futures)) >>= fun () ->

    EagerGameplay.setup_resources p game settings;

    (* don't include the initial passes in the move count! *)
    game.num_moves := p - game.num_punters;

    play_game p c game `Saving

  let create_socket () =
    let open Lwt_unix in
    let sock = socket (socket_domain ()) SOCK_STREAM 0 in
    (* The SO_REUSEADDR option allows us to use the address+port even
       if it hasn't properly timed out yet (after a previous run of
       the program) . *)
    setsockopt sock SO_REUSEADDR true;
    connect sock (address ()) >>= fun () ->
    return sock

  let go () =
    let open Lwt in
    Lwt_main.run
      begin
        Log.init () >>= fun () ->
        create_socket () >>= fun sock ->
        setup_game sock >>= fun () ->
        Lwt_unix.close sock
      end
end

(* Offline version of the eager client *)
module OfflineEagerClient = struct
  open Lwt

  type chan = {ic: Lwt_io.input_channel; oc: Lwt_io.output_channel}

  type connections = punter array

  let send c json =
    let s = Json.json_to_string json in
    Log.debug (lazy ("Client sending: " ^ s)) >>= fun () ->
    BoundedIO.send c.oc s
  let recv c      =
    BoundedIO.recv c.ic >>= fun s ->
    Log.debug (lazy ("Client receiving: "^ s)) >>= fun () ->
    return (Json.from_string s)

  (* FIXME: abstract out duplicate code between offline and online eager clients *)

  (* can't be bothered to implement anything here *)
  let stop c msg = return ()

  let (++) = Json.join

  let rec play_move c msg =
    let open Yojson.Safe.Util in
    let state = Json.input_state msg in

    let (p, n, map, settings) = Json.input_game_state state in
    let goal = Json.input_goal state in
    let move_history = Json.input_moves state in

    Game.apply_settings settings;
    let game = Json.create_game settings n map in

    EagerGameplay.setup_resources p game settings;
    let new_moves = EagerGameplay.sort_moves p game.num_punters (Json.input_move_request msg) in

    let moves = move_history @ new_moves in

    (* don't include the initial passes in the move count! *)
    game.num_moves := p - game.num_punters;
    List.iter (fun move -> Game.make_move game move; incr game.num_moves) moves;
    let {credit; options} = Game.extract_resource p game in

    Log.debug (lazy ("Number of moves: "^string_of_int !(game.num_moves))) >>= fun () ->
    Log.debug (lazy ("Credit: "^string_of_int credit)) >>= fun () ->
    Log.debug (lazy ("Options: "^string_of_int options)) >>= fun () ->
    Log.debug (lazy ("Goal: "^if goal = `Splurging then "splurging" else "saving")) >>= fun () ->

    let (move, _resources), goal = EagerGameplay.select_move p game ({credit; options}, goal) in

    let state = Json.output_game_state p n map settings ++
                  Json.output_goal goal ++
                  Json.output_moves moves in

    let json = Json.with_state (Json.output_move move) state in
    send c json

  let setup_game c msg =
    let (p, n, map, settings) = Json.input_game_state msg in
    Game.apply_settings settings;
    let game = Json.create_game settings n map in
    let futures = EagerGameplay.setup_futures p game in

    let state = Json.output_game_state p n map settings ++
                Json.output_goal `Saving ++
                Json.output_moves [] in

    send c (Json.with_state (Json.output_ready p ++ Json.output_futures futures) state)

  let handshake c =
    (* handshake *)
    send c (Json.output_me "eager punter") >>= fun () ->
    recv c >>= fun you ->
    ignore(Json.input_you you);
    return ()

  let dispatch () =
    let open Yojson.Safe.Util in
    let c = {ic=Lwt_io.stdin; oc=Lwt_io.stdout} in

    handshake c >>= fun () ->
    recv c >>= fun msg ->

    Lwt.catch
      (fun () ->
         match Json.input_server_message_type msg with
         | `Setup -> setup_game c msg
         | `Move  -> play_move c msg
         | `Stop  -> stop c msg)
      (function
        | Json.MalformedServerMessage(json) ->
          Log.error ("Malformed server message: " ^ Json.json_to_string json) >>= fun () ->
          return ()
        | e -> fail e)

  let go () =
    let open Lwt in
    Lwt_main.run
      begin
        Log.init () >>= fun () ->
        Log.debug (lazy ("Client settings: " ^ Game.string_of_settings (read_settings ()))) >>= fun () ->
        dispatch ()
      end
end

(* Punter communication interface *)
module type PCI = sig
  type init_data
  type pcon
  type connections

  val send : pcon -> json -> unit Lwt.t
  val recv : pcon -> json Lwt.t

  val with_punter_body : Game.t -> connections -> int -> (pcon -> unit Lwt.t) -> unit Lwt.t

  (* prepare moves to send to the client *)
  val prepare_moves : Game.punter -> connections -> Game.move array -> Game.move list
  (* a move has been received from the client *)
  val register_move : Game.punter -> connections -> unit
  (* a timeout occurred *)
  val register_timeout : float -> Game.punter -> Game.move array -> connections -> unit Lwt.t

  val kill_connection : Game.punter -> connections -> unit Lwt.t

  val go : Game.t -> Score.data -> init_data -> (Game.t -> Score.data -> connections -> unit Lwt.t) -> unit
end

module Server (I : PCI) : sig
  val go : Game.t -> Score.data -> I.init_data -> unit
end = struct
  open Lwt
  open I

  let rec sequence =
    function
    | [] -> return ()
    | comp :: comps -> comp >>= fun () -> sequence comps

  (* interpret JSON as a move *)
  let interpret_move p move =
    try
      return (Json.input_move move)
    with
      Json.MalformedMove(json, msg) ->
      Log.error (punter_prefix p^" requested malformed move: "^json) >>= fun () ->
      Log.error (msg) >>= fun () ->
      return (Pass p)

  (* send a collection of moves (for punters) *)
  let send_moves p connections c moves =
    send c (Json.output_move_request (prepare_moves p connections moves))

  let with_punter ?playing:(playing=true) game connections p f =
    if game.status.(p) = Zombie then
      if playing then
        (* don't try to interact with zombie punters *)
        Log.info ("Ignoring zombie punter " ^ string_of_int p) >>= fun () ->
        (* JSON output a zombie move as a pass *)
        Log.json (Json.output_move (Pass p))
      else
        return ()
    else
      Lwt.catch
        (fun () ->
          with_punter_body game connections p f)
        (fun e ->
           Log.json (Json.output_zombie p) >>= fun () ->
           Log.error (punter_prefix p ^ " just became a zombie") >>= fun () ->
           Log.error ("Eating exception: "^Printexc.to_string e) >>= fun () ->
           game.status.(p) <- Zombie;
           kill_connection p connections)

  let report_punter_statuses game =
    let status p =
      match game.status.(p) with
      | Active {credit; options} ->
        begin
          if !Settings.splurges then
            Log.info (punter_prefix p^" credit: "^string_of_int credit)
          else
            return ()
        end >>= fun () ->
        begin
          if !Settings.options then
            Log.info (punter_prefix p^" options: "^string_of_int options)
          else
            return ()
        end
      | Zombie ->
        Log.info (punter_prefix p^" is a zombie") in
    let rec statuses p =
      if p = game.num_punters then
        return ()
      else
        status p >>= fun () -> statuses (p+1) in
    Log.json (Json.output_punter_statuses (game.status)) >>= fun () ->
    statuses 0

  let finish_game game scoring_data moves connections p_last =
    let scores = Score.calc_scores scoring_data game.futures game.punter_graphs in
    let rec bye p =
      let p = p mod game.num_punters in
      (* p doesn't need to know their own last move *)
      moves.(p) <- Pass p;
      let stop = Json.output_stop moves (Array.map fst scores) in
      with_punter
        ~playing:false
        game
        connections
        p
        (fun pcon -> send pcon stop) >>= fun () ->
      if p = p_last then
        Log.info ("Game finished") >>= fun () ->
        Score.report_scores scores >>= fun () ->
        report_punter_statuses game
      else
        bye (p+1) in
    bye (p_last+1)

  let with_timeout t p pcon pending_moves connections f g =
    Lwt.catch
      (fun () ->
         Lwt.pick [Lwt_unix.timeout t; f] >>= g)
      (function
        | Lwt_unix.Timeout ->
          Log.json (Json.output_punter_timeout p) >>= fun () ->
          Log.error (punter_prefix p^" timed out") >>= fun () ->
          (* send_timeout pcon t >>= fun () -> *)
          register_timeout t p pending_moves connections >>= fun () -> g (Json.output_move (Pass p))
        | e ->
          Log.error ("Forwarding exception: "^Printexc.to_string e) >>= fun () ->
          fail e)

  let rec play_game game scoring_data connections p moves =
    let identity_check p q =
      if p <> q then
        Log.info (punter_prefix p^
                    " is confused about their identity ("^
                      string_of_int q^")")
      else
        return () in
    Log.info (punter_prefix p^" to move") >>= fun () ->
    let moves' = Array.copy moves in
    (* moves always happen: an illegal move is interpreted as a pass -
       the default *)
    moves.(p) <- Pass p;
    with_punter
      ~playing:true
      game
      connections
      p
      (fun pcon ->
        with_timeout
          !Settings.move_timeout p pcon moves' connections
          (send_moves p connections pcon moves' >>= fun () ->
           recv pcon >>= fun move ->
           (* record that the client sent some kind of move (valid
               json at least) *)
           register_move p connections;
           return move)
          (fun move ->
            interpret_move p move >>= fun move ->
            (* each move yields one credit *)
            Game.incr_credit p game;
            begin
              match move with
              | Claim (q, (i, j)) ->
                 identity_check p q >>= fun () ->
                 begin
                   try
                     Game.claim game (p, (i, j));
                     moves.(p) <- move;
                     return ()
                   with
                   | Game.IllegalMove(msg) ->
                      Log.error ("Illegal move: " ^ msg)
                 end
              | Pass q ->
                 identity_check p q >>= fun () ->
                 Game.pass game p;
                 return ()
              | Splurge (q, sites) ->
                 identity_check p q >>= fun () ->
                 begin
                   try
                     Game.splurge game p sites;
                     moves.(p) <- move;
                     return ()
                   with
                   | Game.IllegalMove(msg) ->
                      Log.error ("Illegal move: " ^ msg)
                 end
              | Option (q, (i, j)) ->
                 identity_check p q >>= fun () ->
                 begin
                   try
                     Game.option game (p, (i, j));
                     moves.(p) <- move;
                     return ()
                   with
                   | Game.IllegalMove(msg) ->
                      Log.error ("Illegal move: " ^ msg)
                 end
            end >>= fun () ->
            (* report the actual move that is made *)
            Log.json (Json.without_state (Json.output_move (moves.(p)))) >>= fun () ->
            Log.info (Game.string_of_move (moves.(p))))) >>= fun () ->
    game.num_moves := !(game.num_moves) + 1;
    if Game.finished game then
      begin
        Log.json (Json.output_gameplay `Stop) >>= fun () ->
        finish_game game scoring_data moves connections p
      end
    else
      play_game game scoring_data connections ((p+1) mod game.num_punters) moves

  let start_game game scoring_data connections =
    let map = Json.output_map game in
    (* Log.info ("Map: "^Json.json_to_string map) >>= fun () -> *)
    let initial_moves = Array.init game.num_punters (fun p -> Pass p) in
    let rec graph_to_punters p =
      if p = game.num_punters then
        return ()
      else
        with_punter
          ~playing:false
          game
          connections
          p
          (fun pcon ->
             Lwt.catch
               (fun () ->
                  let settings = Game.read_settings () in
                  let init = Json.output_game_state p (game.num_punters) map settings in
                  with_timeout
                    (!Settings.setup_timeout) p pcon initial_moves connections
                    (send pcon init >>= fun () ->
                     recv pcon)
                    (fun msg ->
                       ignore (Json.input_ready msg);
                       if !Settings.futures then
                         begin
                           let futures = game.futures.(p) in
                           let good_futures, bad_futures =
                             (* if there are duplicates then the last one is taken *)
                             List.fold_left
                               (fun (good_futures, bad_futures) (source, target) ->
                                  try
                                    Game.check_future game source target;
                                    Hashtbl.add futures source (Some target);
                                    ((source, target)::good_futures, bad_futures)
                                  with
                                  | Game.IllegalFuture(msg) ->
                                    (good_futures, ((source, target), msg)::bad_futures))
                               ([], [])
                               (Json.input_futures msg) in
                           (* report bad futures *)

                           (* sort the futures before logging *)
                           let bad_futures =
                             List.sort (fun ((m, _), _) ((m', _), _) -> m - m') bad_futures in
                           let good_futures =
                             List.sort (fun (m, _) (m', _) -> m - m') good_futures in

                           List.fold_left
                             (fun errors ((source, target), msg) ->
                                errors >>= fun () ->
                                Log.error ("Illegal future ("^string_of_int source^", "^string_of_int target^"): "^msg))
                             (return ())
                             bad_futures >>= fun () ->

                           (* output good futures to JSON *)
                           Log.json (Json.join (`Assoc [("punter", `Int p)]) (Json.output_futures good_futures))
                         end
                       else
                         return ()))
               (fun e ->
                  Log.json (Json.output_zombie p) >>= fun () ->
                  Log.error (punter_prefix p^ " just became a zombie") >>= fun () ->
                  Log.error ("Eating exception: "^Printexc.to_string e) >>= fun () ->
                  game.status.(p) <- Zombie;
                  return ())) >>= fun () ->
        graph_to_punters (p+1) in
    (* Using join might lead to greater parallelism for the online
       server, but it isn't so helpful for making the offline
       evaluation deterministic *)
    (* join (graph_to_punters 0) *)
    graph_to_punters 0 >>= fun () ->
    Log.json (Json.output_gameplay `Start) >>= fun () ->
    play_game game scoring_data connections 0 initial_moves

  let go game scoring_data init_data = go game scoring_data init_data start_game
end

(* Punter communication interface using the sockets API *)
module OnlineServer : PCI with type init_data = unit = struct
  open Lwt

  type client_type = Punter | Broken

  type init_data = unit

  type chan = {ic: Lwt_io.input_channel; oc: Lwt_io.output_channel; timeouts: int}
  type pcon = chan
  type connections = chan array

  let socket_domain () = Lwt_unix.PF_INET
  let address () = Lwt_unix.ADDR_INET(Unix.inet_addr_of_string (!Settings.address), !Settings.port)

  let backlog = 10

  let send c json =
    let s = Json.json_to_string json in
    Log.debug (lazy ("Server sending: "^s)) >>= fun () ->
    BoundedIO.send c.oc s
  let recv c      =
    BoundedIO.recv c.ic >>= fun s ->
    Log.debug (lazy ("Server received: "^ s)) >>= fun () ->
    return (Json.from_string s)

  let send_timeout c t =
    send c (Json.output_timeout t)

  let close chan =
    Lwt_io.close chan.ic >>= fun () ->
    Lwt_io.close chan.oc

  let kill_connection p connections =
    Lwt.catch
      (fun () ->
         Log.info ("Killing connection to punter "^string_of_int p) >>= fun () ->
         close connections.(p))
      (fun e ->
         Log.error ("Error killing connection to punter "^string_of_int p) >>= fun () ->
         Log.error ("Eating exception: "^Printexc.to_string e))

  exception TooManyTimeouts of int

  let prepare_moves _p _connections moves = Array.to_list moves
  let register_move _p _connections = ()

  let register_timeout t p _moves connections =
    let punter = connections.(p) in
    let timeouts = punter.timeouts+1 in
    connections.(p) <- {punter with timeouts=timeouts};
    (* Send a timeout message *)
    send_timeout connections.(p) t >>= fun () ->
    if timeouts >= !Settings.max_timeouts then
      fail (TooManyTimeouts timeouts)
    else
      return ()

  let init_connection game conn i num_punters =
    let fd, sockaddr = conn in
    let c = {ic=Lwt_io.of_fd Lwt_io.Input fd;
             oc=Lwt_io.of_fd Lwt_io.Output fd;
             timeouts=0} in
    Lwt.catch
      (fun () ->
         recv c >>=
         fun me ->
         begin
           match Json.input_me me with
           | name                             ->
             return name
           | exception Yojson.Json_error(msg) ->
             Log.error (punter_prefix i^" requested malformed name: "^Json.json_to_string me) >>= fun () ->
             Log.error (msg) >>= fun () -> return "garbled name"
         end >>= fun name ->
         Log.info ("New punter: " ^ name ^ ", with punter id: " ^ string_of_int i) >>= fun () ->
         send c (Json.output_you name) >>= fun () ->
         return (Punter, c))
      (fun e ->
         Log.error ("Failed to initiate conversation") >>= fun () ->
         Log.error ("Eating exception: "^Printexc.to_string e) >>= fun () ->
         return (Broken, c))

  let create_socket () =
    let open Lwt_unix in
    let sock = socket (socket_domain ()) SOCK_STREAM 0 in
    (* The SO_REUSEADDR option allows us to use the address+port even
       if it hasn't properly timed out yet (after a previous run of
       the program). *)
    setsockopt sock SO_REUSEADDR true;
    (* I was tempted to try to indirectly limit the maximum amount of data that may be sent
     in one go by insisting that any read operation cannot take more
     than a second. Unfortunately this seems to break everything! *)
    (* setsockopt_float sock SO_RCVTIMEO 1.0; *)
    bind sock @@ address () >>= fun () ->
    (listen sock backlog; return sock)

  (* send move along a channel *)
  let send_move c move = send c (Json.output_move move)

  let with_punter_body game connections p f =
    f connections.(p)

  let setup_game game scoring_data sock start_game =
    let rec setup ps connected_punters =
      if connected_punters = game.num_punters then
        Lwt_unix.close sock >>= fun _ -> (* Close acceptor socket *)
        start_game game scoring_data (Array.of_list (List.rev ps))
      else
        Log.info ("Waiting for punter "^string_of_int connected_punters^"...") >>= fun () ->
        Lwt_unix.accept sock >>=
        fun conn ->
        init_connection game conn connected_punters game.num_punters >>=
        fun (client_kind, c) ->
        match client_kind with
        | Punter ->
           setup (c::ps) (connected_punters+1)
        | Broken ->
           let fd, _ = conn in
           Lwt_unix.close fd >>= fun () ->
           setup ps connected_punters
    in
    setup [] 0

  let go game scoring_data () start_game =
    Lwt_main.run
      begin
        Log.init () >>= fun () ->
        Log.info ("Base score: " ^ string_of_int (Score.calc_total scoring_data (Game.empty_futures game.mines) game.base)) >>= fun () ->
        Log.info ("Server settings: " ^ Game.string_of_settings (read_settings ())) >>= fun () ->
        create_socket () >>= fun sock ->
        (Repl.go game scoring_data <?> setup_game game scoring_data sock start_game)
      end
end

(* Punter communication interface using pipes to local processes *)
module OfflineServer : PCI with type init_data = string list = struct
  open Lwt
  type init_data = string list

  type chan = {ic: Lwt_io.input_channel; oc: Lwt_io.output_channel}

  type pcon = {p: int; process: Lwt_process.process_none; chan: chan}
  type punter = {prog: string; timeouts: int; pending_moves: Game.move list}

  type connections = punter array

  let state = Array.init !Settings.num_punters (fun _p -> None)
  let update_state p json =
    state.(p) <- Some (Json.input_state json)

  let send_no_state pcon json =
    Log.debug (lazy ("Server sending (no state): " ^ Json.json_to_string json)) >>= fun () ->
    BoundedIO.send pcon.chan.oc (Json.json_to_string json)
  let recv_no_state pcon      =
    BoundedIO.recv pcon.chan.ic >>= fun s ->
    Log.debug (lazy ("Server received (no state): " ^ s)) >>= fun () ->
    return (Json.from_string s)

  let send pcon json =
    begin
      match state.(pcon.p) with
      | None ->
         Log.debug (lazy ("Server sending (no state): " ^ Json.json_to_string json)) >>= fun () ->
         return @@ Json.json_to_string json
      | Some state ->
         Log.debug (lazy ("Server sending: " ^ Json.json_to_string (Json.with_state json state))) >>= fun () ->
         return @@ Json.json_to_string (Json.with_state json state)
    end >>= fun s ->
    BoundedIO.send pcon.chan.oc s
  let recv pcon      =
    BoundedIO.recv pcon.chan.ic >>= fun s ->
    Log.debug (lazy ("Server received: " ^ s)) >>= fun () ->
    let json = Json.from_string s in
    (* Log.debug (lazy ("Server received: " ^ Json.json_to_string (Json.without_state json))) >>= fun () -> *)
    update_state pcon.p json;
    return json

  let close chan =
    Lwt_io.close chan.ic >>= fun () ->
    Lwt_io.close chan.oc

  exception TooManyTimeouts of int

  let kill_connection p connections =
    (* nothing to do as the connection will be killed anyway *)
    return ()

  let prepare_moves p connections moves = connections.(p).pending_moves @ Array.to_list moves
  let register_move p connections = connections.(p) <- {connections.(p) with pending_moves = []}

  let register_timeout _t p moves connections =
    let punter = connections.(p) in
    let timeouts = punter.timeouts+1 in
    if timeouts >= !Settings.max_timeouts then
      fail (TooManyTimeouts timeouts)
    else
      begin
        Log.info (punter_prefix p^" pending moves: ["^
                  String.concat ", " (List.map
                                        (Game.string_of_move ~short:true)
                                        (punter.pending_moves @ Array.to_list moves))^"]") >>= fun () ->
        connections.(p) <- {punter with timeouts; pending_moves = punter.pending_moves @ Array.to_list moves};
        return ()
      end

  let run_prog p punter =
    let stdin_pipe = Lwt_unix.pipe_out () in
    let stdout_pipe = Lwt_unix.pipe_in () in

    let icr = `FD_move (fst stdin_pipe) in
    let ocr = `FD_move (snd stdout_pipe) in

    Log.info ("Running punter "^string_of_int p^" program: "^punter.prog) >>= fun () ->

    let cmd =
      match !Settings.round with
      | None ->
        ("", [|"./go.sh"; punter.prog|])
      | Some round ->
        ("", [|"./go.sh"; punter.prog; round|]) in

    let process =
      Lwt_process.open_process_none
        ~timeout:10.0
        ~stdin:icr
        ~stdout:ocr
        (* TODO: pipe stderr somewhere? *)
        (* ~stderr:(`FD_copy (Lwt_unix.unix_file_descr Lwt_unix.stderr)) *)
        cmd in
    let chan =
      {ic = Lwt_io.of_fd Lwt_io.Input (fst stdout_pipe);
       oc = Lwt_io.of_fd Lwt_io.Output (snd stdin_pipe)} in
    return {p; process; chan}

  let handshake p c =
    Lwt.catch
      (fun () ->
         (* don't wait forever for a handshake *)
         Lwt.pick [Lwt_unix.timeout !Settings.handshake_timeout; recv_no_state c] >>=
         fun me ->
         begin
           match Json.input_me me with
           | name                             -> return name
           | exception Yojson.Json_error(msg) ->
             Log.error (punter_prefix p^" requested malformed name: "^Json.json_to_string me) >>= fun () ->
             Log.error (msg) >>= fun () -> return "garbled name"
         end >>= fun name ->
         Log.debug (lazy ("Handshake: Punter: " ^ name ^ "; punter id: " ^ string_of_int p)) >>= fun () ->
         send_no_state c (Json.output_you name) >>= fun () ->
         return ())
      (fun e ->
         Log.error ("Failed to initiate conversation") >>= fun () ->
         Log.error ("Eating exception: "^Printexc.to_string e) >>= fun () ->
         Log.error ("Commiting suicide") >>= fun () ->
         fail Exit)

  let with_punter_body game connections p f =
    run_prog p connections.(p) >>= fun pcon ->
    handshake p pcon >>= fun () ->
    f pcon >>= fun () ->
    Log.info ("Terminating punter "^string_of_int p^" program") >>= fun () ->
    (* close pipe *)
    close pcon.chan >>= fun () ->
    (* send sigkill *)
    pcon.process#terminate;
    if !Settings.round <> None then
      Log.info ("Killing any remaining punter processes") >>= fun () ->
      let _kill_process = Unix.system "./kill.sh" in
      return ()
    else
      return ()

  let setup_game game scoring_data progs start_game =
    let connections = Array.of_list (List.mapi (fun p prog -> {prog=prog; timeouts=0; pending_moves=[]}) progs) in
    start_game game scoring_data connections

  let report_map game =
    let map =
      match !Settings.map_path with
      | None -> "default.json"
      | Some s -> Filename.basename s in
    Log.info ("Map: "^map) >>= fun () ->
    Log.info ("Number of sites: "^string_of_int (G.nb_vertex game.base)) >>= fun () ->
    Log.info ("Number of rivers: "^string_of_int (G.nb_edges game.base)) >>= fun () ->
    Log.info ("Number of mines: "^string_of_int (List.length game.mines)) >>= fun () ->
    Log.json (`Assoc [("map", `String map)])

  let report_progs progs =
    let rec report p =
      function
      | []              -> return ()
      | (prog :: progs) ->
        Log.info ("Punter "^string_of_int p^" is "^prog) >>= fun () ->
        Log.json (`Assoc [("punter", `Int p); ("team", `String prog)]) >>= fun () ->
        report (p+1) progs in
    report 0 progs

  let report_settings settings =
    Log.info ("Extensions: "^Game.string_of_settings settings) >>= fun () ->
    Log.json (Json.output_settings settings)

  let go game scoring_data progs start_game =
    let open Lwt in
    Lwt_main.run
      begin
        Log.init () >>= fun () ->
        let settings = read_settings () in
        report_settings settings >>= fun () ->
        report_map game >>= fun () ->
        Log.info ("Base score: " ^ string_of_int (Score.calc_total scoring_data (Game.empty_futures game.mines) game.base)) >>= fun () ->
        report_progs progs >>= fun () ->
        Repl.go game scoring_data <?> setup_game game scoring_data progs start_game
      end
end

(* Use Dijkstra's algorithm to calculate all shortest paths from a
   mine.

   Requires Simon Fowler's patched version of the ocamlgraph
   library:

     https://github.com/simonjf/ocamlgraph
*)
let all_shortest_paths g i =
  Dijkstra.all_shortest_paths g i

let main () =
  if !Settings.eager then
    begin
      if !Settings.offline then
        begin
          OfflineEagerClient.go ();
          exit (-1)
        end
      else
        begin
          OnlineEagerClient.go ();
          exit (-1)
        end
    end
  else
    (* Create a fresh game *)
    let game = Json.create_game (Game.read_settings ()) !Settings.num_punters (Json.from_string !Settings.map) in
    (* Calculate scoring data *)
    let scoring_data =
      (* compute all shortest paths from each mine *)
      let scoring_data =
        List.fold_left
          (fun scoring_data i ->
             (i, all_shortest_paths game.base i) :: scoring_data)
          []
          game.mines in
      scoring_data in
    (* Start the server *)
    if !Settings.offline then
      let module Offline = Server(OfflineServer) in
      Offline.go game scoring_data !Settings.progs
    else
      let module Online = Server(OnlineServer) in
      Online.go game scoring_data ()

let _ = main ()
