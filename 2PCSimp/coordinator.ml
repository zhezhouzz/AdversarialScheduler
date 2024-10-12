open Messages
open Zutils
open Zdatatype

let coordinator_handler self ({ source; payload; dest } as msg) =
  let send d payload = { source = dest; dest = d; payload } in
  (* let send_back payload = { source = dest; dest = source; payload } in *)
  match payload with
  | ReadReq { clientId } ->
      let dest' = choose_from_set self.participants in
      Some ([ send dest' (GetReq { clientId }) ], self)
  | WriteReq { v } -> (
      match self.state with
      | Handling _ -> None
      | WaitForactions ->
          let state = Handling { source; v; counter = 0 } in
          let msgs =
            IntSet.fold
              (fun d l -> send d (PutReq { v }) :: l)
              self.participants []
          in
          Some (msgs, { self with state }))
  | PutResp { stat } -> (
      match self.state with
      | WaitForactions -> Some ([], self)
      | Handling { source = s; counter; v } -> (
          let finalize stat =
            let payload = if stat then Commit else Abort in
            let msgs =
              IntSet.fold (fun d l -> send d payload :: l) self.participants []
            in
            let msgs = msgs @ [ send s (WriteResp { v; stat }) ] in
            Some (msgs, { self with state = WaitForactions })
          in
          match stat with
          | true ->
              if counter < IntSet.cardinal self.participants - 1 then
                let state = Handling { v; counter = counter + 1; source = s } in
                Some ([], { self with state })
              else finalize true
          | false -> finalize false))
  | _ -> _die_with [%here] (show_message msg)
