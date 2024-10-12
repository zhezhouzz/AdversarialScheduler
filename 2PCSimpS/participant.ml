open Messages
open Zutils
(* open Zdatatype *)

let participant_handler self ({ source; payload; dest } as msg) =
  let send d payload = { source = dest; dest = d; payload } in
  let send_back payload = { source = dest; dest = source; payload } in
  match payload with
  | Abort -> ([], { self with pendingWrite = None })
  | Commit -> ([], { localKvStore = self.pendingWrite; pendingWrite = None })
  | PutReq { v } ->
      let self' = { self with pendingWrite = Some v } in
      if Random.bool () then ([ send_back (PutResp { stat = true }) ], self')
      else ([ send_back (PutResp { stat = false }) ], self)
  | GetReq { clientId } ->
      let payload =
        match self.localKvStore with
        | None -> ReadResp { v = 0 }
        | Some v -> ReadResp { v }
      in
      ([ send clientId payload ], self)
  | _ -> _die_with [%here] (show_message msg)
