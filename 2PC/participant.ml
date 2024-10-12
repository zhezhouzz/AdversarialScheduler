open Messages
open Zutils
open Zdatatype

let participant_handler self ({ source; payload; dest } as msg) =
  let send d payload = { source = dest; dest = d; payload } in
  let send_back payload = { source = dest; dest = source; payload } in
  match payload with
  | AbortTrans { transId } ->
      ( [],
        {
          self with
          pendingWriteTrans = IntMap.remove transId self.pendingWriteTrans;
        } )
  | CommitTrans { transId } ->
      let localKvStore =
        match IntMap.find_opt self.pendingWriteTrans transId with
        | None -> self.localKvStore
        | Some trans ->
            StrMap.update trans.key
              (function _ -> Some trans)
              self.localKvStore
      in
      ( [],
        {
          localKvStore;
          pendingWriteTrans = IntMap.remove transId self.pendingWriteTrans;
        } )
  | PrepareReq { key; value; transId } ->
      let self' =
        {
          self with
          pendingWriteTrans =
            IntMap.update transId
              (fun _ -> Some { key; value; transId })
              self.pendingWriteTrans;
        }
      in
      let if_update =
        match StrMap.find_opt self.localKvStore key with
        | None -> true
        | Some trans -> transId > trans.transId
      in
      if if_update then
        ([ send_back (PrepareResp { transId; status = SUCCESS }) ], self')
      else ([ send_back (PrepareResp { transId; status = ERROR }) ], self)
  | ReadTransReq { clientId; key; _ } ->
      let payload =
        match StrMap.find_opt self.localKvStore key with
        | None ->
            ReadTransResp { key; transId = -1; value = -1; status = ERROR }
        | Some trans ->
            ReadTransResp
              {
                key;
                value = trans.value;
                transId = trans.transId;
                status = SUCCESS;
              }
      in
      ([ send clientId payload ], self)
  (* | ReadTransReq { clientId; key; transId } -> *)
  (*     let payload = *)
  (*       match IntMap.find_opt self.pendingWriteTrans transId with *)
  (*       | Some trans -> *)
  (*           ReadTransResp *)
  (*             { *)
  (*               key; *)
  (*               value = trans.value; *)
  (*               transId = trans.transId; *)
  (*               status = SUCCESS; *)
  (*             } *)
  (*       | None -> ( *)
  (*           match StrMap.find_opt self.localKvStore key with *)
  (*           | None -> *)
  (*               ReadTransResp { key; transId = -1; value = -1; status = ERROR } *)
  (*           | Some trans -> *)
  (*               ReadTransResp *)
  (*                 { *)
  (*                   key; *)
  (*                   value = trans.value; *)
  (*                   transId = trans.transId; *)
  (*                   status = SUCCESS; *)
  (*                 }) *)
  (*     in *)
  (*     ([ send clientId payload ], self) *)
  | _ -> _die_with [%here] (show_message msg)
