open Messages
open Zutils

let num_write = 1

let client_handler self { payload; dest; _ } =
  let send payload = { source = dest; dest = self.coordinator; payload } in
  let write () = send (WriteReq { v = Random.int 3 }) in
  let read () = send (ReadReq { clientId = dest }) in
  match payload with
  | ReadResp _ | WriteResp _ -> ([], self)
  | Gen -> (List.init num_write (fun _ -> write ()) @ [ read (); read () ], self)
  | _ -> _die [%here]

let random_key = List.nth [ "a"; "b"; "c"; "d"; "e" ] @@ Random.int 3
let random_ = Random.int 10

(* let random_client_handler self { payload; dest; _ } = *)
(*   let send payload = { source = dest; dest = self.coordinator; payload } in *)
(*   let key = "zz" in *)
(*   let write  = *)
(*     send (WriteReq {  v = Random.int 3;  }) *)
(* let write  = *)
(*   send (WriteReq {  v = Random.int 3;  }) *)
(*   in *)
(*   match payload with *)
(*   | ReadResp _ -> *)
(*       if self.fuel <= 0 then ([], self) *)
(*       else *)
(*         let  = self. + 1 in *)
(*         let fuel = self.fuel - 1 in *)
(*         ([ write  ], { self with fuel;  }) *)
(*   | WriteResp _ -> *)
(*       ( [ send (ReadReq {  clientId = dest;  = self. }) ], *)
(*         self ) *)
(*   | Gen -> ([ write self. ], self) *)
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
  let participants = List.init 2 (fun i -> i) in
  let machines =
    IntMap.of_seq @@ List.to_seq
    @@ List.map (fun i -> (i, mkParticipant)) participants
  in
  let machines, coordinator =
    add_machine machines @@ mkCoordinator (IntSet.of_list participants)
  in
  let machines, c1 = add_machine machines @@ mkClient coordinator in
  (* let machines, c2 = add_machine machines @@ mkClient coordinator in *)
  let dummy = IntMap.cardinal machines in
  let pendingMsgs =
    List.map (fun dest -> { source = dummy; dest; payload = Gen }) [ c1 ]
  in
  run machines pendingMsgs

let strongConsistency (trace : message list) =
  let view = Hashtbl.create 10 in
  let write (client, v) = Hashtbl.add view client v in
  let read (client, v) =
    match Hashtbl.find_opt view client with
    | None -> ()
    | Some v' ->
        if v != v' then (
          printMsgs trace;
          Printf.printf "[assertionFailure]: client %i, v: %i v.s. %i\n" client
            v' v;
          _die [%here])
  in
  let parse = function
    | { dest; payload = WriteResp { v; stat = true; _ }; _ } -> write (dest, v)
    | { dest; payload = ReadResp { v; _ }; _ } -> read (dest, v)
    | _ -> ()
  in
  let () = List.iter parse trace in
  (* let f = function *)
  (*   | { dest; payload = WriteResp _; _ } *)
  (*   | { dest; payload = ReadResp _; _ } *)
  (*     when dest == 4 -> *)
  (*       true *)
  (*   | _ -> false *)
  (* in *)
  (* printMsgs @@ List.filter f trace *)
  ()

let readConsistency (trace : message list) =
  let view = Hashtbl.create 10 in
  let counterW = ref 0 in
  let write (client, v) = Hashtbl.add view client v in
  let read (client, v) =
    match Hashtbl.find_opt view client with
    | None -> write (client, v)
    | Some v' ->
        if !counterW >= num_write && v != v' then (
          printMsgs trace;
          Printf.printf "[assertionFailure]: client %i, v: %i v.s. %i\n" client
            v' v;
          _die [%here])
  in
  let parse = function
    | { payload = WriteResp _; _ } -> counterW := !counterW + 1
    | { dest; payload = ReadResp { v; _ }; _ } -> read (dest, v)
    | _ -> ()
  in
  let () = List.iter parse trace in
  (* let f = function *)
  (*   | { dest; payload = WriteResp _; _ } *)
  (*   | { dest; payload = ReadResp _; _ } *)
  (*     when dest == 4 -> *)
  (*       true *)
  (*   | _ -> false *)
  (* in *)
  (* printMsgs @@ List.filter f trace *)
  ()
