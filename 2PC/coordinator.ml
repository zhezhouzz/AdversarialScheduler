open Messages
open Zutils
open Zdatatype

let coordinator_handler self ({ source; payload; dest } as msg) =
  let send d payload = { source = dest; dest = d; payload } in
  let send_back payload = { source = dest; dest = source; payload } in
  match payload with
  | ReadTransReq _ ->
      let dest' = choose_from_set self.participants in
      Some ([ send dest' payload ], self)
  | WriteTransReq { key; value; transId } -> (
      match self.state with
      | Handling _ -> None
      | WaitForTransactions ->
          let mkMsg status =
            send_back (WriteTransResp { status; key; value; transId })
          in
          if IntSet.mem transId self.seenTransIds then
            Some ([ mkMsg TIMEOUT ], self)
          else
            let state = Handling { source; transId; key; value; counter = 0 } in
            let msgs =
              IntSet.fold
                (fun d l -> send d (PrepareReq { key; value; transId }) :: l)
                self.participants []
            in
            Some (msgs, { self with state }))
  | PrepareResp { transId; status } -> (
      match self.state with
      | WaitForTransactions -> Some ([], self)
      | Handling { source = s; transId = id; counter; key; value } -> (
          let finalizeTrans ifCommit =
            let payload =
              if ifCommit then CommitTrans { transId }
              else AbortTrans { transId }
            in
            let msgs =
              IntSet.fold (fun d l -> send d payload :: l) self.participants []
            in
            let status = if ifCommit then SUCCESS else ERROR in
            let msgs =
              msgs @ [ send s (WriteTransResp { key; value; transId; status }) ]
            in
            let seenTransIds = IntSet.add transId self.seenTransIds in
            Some (msgs, { self with state = WaitForTransactions; seenTransIds })
          in
          if transId != id then Some ([], self)
          else
            match status with
            | SUCCESS ->
                if counter < IntSet.cardinal self.participants - 1 then
                  let state =
                    Handling
                      {
                        key;
                        value;
                        counter = counter + 1;
                        source = s;
                        transId = id;
                      }
                  in
                  Some ([], { self with state })
                else finalizeTrans true
            | _ -> finalizeTrans false))
  | _ -> _die_with [%here] (show_message msg)
