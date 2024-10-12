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

type tTrans = { key : string; value : int; transId : int } [@@deriving show]
type tTransStatus = SUCCESS | ERROR | TIMEOUT [@@deriving show]

type payload =
  | ReadTransReq of { clientId : int; key : string; transId : int }
  | ReadTransResp of {
      key : string;
      value : int;
      transId : int;
      status : tTransStatus;
    }
  | WriteTransReq of tTrans
  | WriteTransResp of {
      transId : int;
      key : string;
      value : int;
      status : tTransStatus;
    }
  | PrepareReq of tTrans
  | PrepareResp of { transId : int; status : tTransStatus }
  | AbortTrans of { transId : int }
  | CommitTrans of { transId : int }
  | Gen
[@@deriving show]

type message = { source : int; dest : int; payload : payload } [@@deriving show]
type event = Send of message | Receive of message [@@deriving show]

type participant = {
  localKvStore : tTrans StrMap.t;
  pendingWriteTrans : tTrans IntMap.t;
}

type coordinatorState =
  | WaitForTransactions
  | Handling of {
      source : int;
      transId : int;
      counter : int;
      key : string;
      value : int;
    }
[@@deriving show]

type coordinator = {
  seenTransIds : IntSet.t;
  participants : IntSet.t;
  state : coordinatorState;
}

type client = { coordinator : int; fuelTrans : int; transId : int }

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

let mkParticipant =
  Participant { localKvStore = StrMap.empty; pendingWriteTrans = IntMap.empty }

let mkCoordinator participants =
  Coordinator
    { seenTransIds = IntSet.empty; state = WaitForTransactions; participants }

let mkClient coordinator transId =
  Client { coordinator; fuelTrans = 1; transId }

let trace_to_msgs =
  List.filter_map (function
    | Send _ | Receive { payload = Gen; _ } -> None
    | Receive msg -> Some msg)

let printMsgs trace =
  Printf.printf "%s\n" @@ List.split_by "\n" show_message trace
