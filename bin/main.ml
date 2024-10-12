open Scheduler
open Zutils
open Zdatatype;;

let () = Random.init 0 in
let _ =
  List.init 10000 (fun i ->
      let () = Printf.printf "test cases %i\n" i in
      readConsistency @@ testCase ())
in
()
