open Messages
open Zutils

let client_handler self { payload; dest; _ } =
  let send payload = { source = dest; dest = self.coordinator; payload } in
  let key = "zz" in
  let write transId =
    send (WriteTransReq { key; value = Random.int 3; transId })
  in
  match payload with
  | ReadTransResp _ ->
      if self.fuelTrans <= 0 then ([], self)
      else
        let transId = self.transId + 1 in
        let fuelTrans = self.fuelTrans - 1 in
        ([ write transId ], { self with fuelTrans; transId })
  | WriteTransResp _ ->
      ( [ send (ReadTransReq { key; clientId = dest; transId = self.transId }) ],
        self )
  | Gen -> ([ write self.transId ], self)
  | _ -> _die [%here]

let random_key = List.nth [ "a"; "b"; "c"; "d"; "e" ] @@ Random.int 3
let random_transId = Random.int 10

(* let random_client_handler self { payload; dest; _ } = *)
(*   let send payload = { source = dest; dest = self.coordinator; payload } in *)
(*   let key = "zz" in *)
(*   let write transId = *)
(*     send (WriteTransReq { key; value = Random.int 3; transId }) *)
(* let write transId = *)
(*   send (WriteTransReq { key; value = Random.int 3; transId }) *)
(*   in *)
(*   match payload with *)
(*   | ReadTransResp _ -> *)
(*       if self.fuelTrans <= 0 then ([], self) *)
(*       else *)
(*         let transId = self.transId + 1 in *)
(*         let fuelTrans = self.fuelTrans - 1 in *)
(*         ([ write transId ], { self with fuelTrans; transId }) *)
(*   | WriteTransResp _ -> *)
(*       ( [ send (ReadTransReq { key; clientId = dest; transId = self.transId }) ], *)
(*         self ) *)
(*   | Gen -> ([ write self.transId ], self) *)
(*   | _ -> _die [%here] *)

open Participant
open Coordinator
open Zdatatype
open Messages

let rec step ({ machines; pendingMsgs; trace } as runtime) =
  (* let () = *)
  (*   Printf.printf "\tState: %s\n" *)
  (*   @@ show_coordinator_state (IntMap.find "die" machines 3) *)
  (* in *)
  let rec aux n =
    if n < 0 then
      let () =
        Printf.printf "%s\n" @@ List.split_by ";" show_message pendingMsgs
      in
      _die [%here]
    else
      let msg, pendingMsgs = extract_from_list pendingMsgs in
      let dest = IntMap.find "die" machines msg.dest in
      let res =
        match dest with
        | Participant self ->
            let msgs, self = participant_handler self msg in
            Some (msgs, Participant self)
        | Coordinator self ->
            let* msgs, self = coordinator_handler self msg in
            Some (msgs, Coordinator self)
        | Client self ->
            let msgs, self = client_handler self msg in
            Some (msgs, Client self)
      in
      match res with Some res -> (msg, pendingMsgs, res) | None -> aux (n - 1)
  in
  if List.is_empty pendingMsgs then runtime
  else
    (* let () = *)
    (*   Printf.printf "pendingMsgs:\n%s\n" *)
    (*   @@ List.split_by "\n" show_message pendingMsgs *)
    (* in *)
    let msg, pendingMsgs, (msgs, dest') = aux 10 in
    let machines = IntMap.update msg.dest (fun _ -> Some dest') machines in
    let newTrace = [ Receive msg ] @ List.map (fun m -> Send m) msgs in
    (* let () = *)
    (*   Printf.printf "newTrace:\n%s\n\n" *)
    (*   @@ List.split_by "\n" show_event newTrace *)
    (* in *)
    let trace = trace @ newTrace in
    let pendingMsgs = pendingMsgs @ msgs in
    step { machines; pendingMsgs; trace }

let run machines pendingMsgs =
  let runtime = step { machines; pendingMsgs; trace = [] } in
  trace_to_msgs runtime.trace

let testCase () =
  let participants = List.init 1 (fun i -> i) in
  let machines =
    IntMap.of_seq @@ List.to_seq
    @@ List.map (fun i -> (i, mkParticipant)) participants
  in
  let machines, coordinator =
    add_machine machines @@ mkCoordinator (IntSet.of_list participants)
  in
  let machines, c1 = add_machine machines @@ mkClient coordinator 0 in
  (* let machines, c2 = add_machine machines @@ mkClient coordinator 10 in *)
  let dummy = IntMap.cardinal machines in
  let pendingMsgs =
    List.map (fun dest -> { source = dummy; dest; payload = Gen }) [ c1 ]
  in
  run machines pendingMsgs

let strongConsistency (trace : message list) =
  let view = Hashtbl.create 10 in
  let write (client, key, value, id) =
    Hashtbl.add view (client, key) (value, id)
  in
  let read (client, key, value, id) =
    match Hashtbl.find_opt view (client, key) with
    | None -> ()
    | Some (value', id') ->
        if value != value' then
          if id' > id then (
            printMsgs trace;
            Printf.printf
              "[assertionFailure]: client %i, key %s, value: %i v.s. %i\n"
              client key value' value;
            _die [%here])
          else write (client, key, value', id')
  in
  let parse = function
    | {
        dest;
        payload = WriteTransResp { key; value; status = SUCCESS; transId; _ };
        _;
      } ->
        write (dest, key, value, transId)
    | {
        dest;
        payload = ReadTransResp { key; value; status = SUCCESS; transId; _ };
        _;
      } ->
        read (dest, key, value, transId)
    | _ -> ()
  in
  let () = List.iter parse trace in
  (* let f = function *)
  (*   | { dest; payload = WriteTransResp _; _ } *)
  (*   | { dest; payload = ReadTransResp _; _ } *)
  (*     when dest == 4 -> *)
  (*       true *)
  (*   | _ -> false *)
  (* in *)
  (* printMsgs @@ List.filter f trace *)
  ()
