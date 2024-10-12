open Zutils
open Zdatatype

let choose_from_list l = List.nth l @@ Random.int (List.length l)

let extract_from_list l =
  let n = Random.int (List.length l) in
  let res = List.nth l n in
  let l = List.filteri (fun i _ -> i != n) l in
  (res, l)

let choose_from_set (s : IntSet.t) =
  choose_from_list @@ List.of_seq @@ IntSet.to_seq s

type payload =
  | ReadReq of { clientId : int }
  | ReadResp of { v : int; stat : bool }
  | WriteReq of { v : int }
  | WriteResp of { v : int; stat : bool }
  | GetReq of { clientId : int }
  | PutReq of { v : int }
  | PutResp of { stat : bool }
  | Abort
  | Commit
  | Gen
[@@deriving show]

type message = { source : int; dest : int; payload : payload } [@@deriving show]
type event = Send of message | Receive of message [@@deriving show]
type participant = { localKvStore : int option; pendingWrite : int option }

type coordinatorState =
  | WaitForactions
  | Handling of { source : int; counter : int; v : int }
[@@deriving show]

type coordinator = { participants : IntSet.t; state : coordinatorState }
type client = { coordinator : int; fuel : int }

type machine =
  | Participant of participant
  | Coordinator of coordinator
  | Client of client

type runtime = {
  machines : machine IntMap.t;
  pendingMsgs : message list;
  trace : event list;
}

let show_coordinator_state = function
  | Coordinator c -> show_coordinatorState c.state
  | _ -> _die [%here]

let add_machine machines m =
  let n = IntMap.cardinal machines in
  let machines = IntMap.add n m machines in
  (machines, n)

let mkParticipant = Participant { localKvStore = None; pendingWrite = None }

let mkCoordinator participants =
  Coordinator { state = WaitForactions; participants }

let mkClient coordinator = Client { coordinator; fuel = 1 }

let trace_to_msgs =
  List.filter_map (function
    | Send _ | Receive { payload = Gen; _ } -> None
    | Receive msg -> Some msg)

let printMsgs trace =
  Printf.printf "%s\n" @@ List.split_by "\n" show_message trace
