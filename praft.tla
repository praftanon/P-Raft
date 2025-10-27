----------------------------- MODULE praft -----------------------------
EXTENDS Naturals, Integers, Sequences, FiniteSets, TLC

(*********************************************************************)
(* 常量：三个域（互不相交）以及无领导标记 + 有界参数                     *)
(*********************************************************************)
CONSTANTS A, B, C, NoLeader
CONSTANTS MAXLEN, TMAX, REPL_BC

ASSUME A \cap B = {} /\ A \cap C = {} /\ B \cap C = {}

(*********************************************************************)
(* 变量                                                               *)
(*********************************************************************)
VARIABLES
  Alive,          \* SUBSET Nodes
  Term,           \* [node -> Nat]     (受 TMAX 约束)
  VotedFor,       \* [node -> node ∪ {NoLeader}]
  Role,           \* [node -> {"Leader","Follower","Candidate"}]
  Leader,         \* ∈ A ∪ {NoLeader}
  Log,            \* [node -> Seq([term: Nat, val: Nat])] (长度受 MAXLEN)
  CommitIndex,    \* Nat               (≤ MAXLEN)
  NextVal         \* Nat               (1..TMAX 循环)

(*********************************************************************)
(* 基本集合/工具                                                      *)
(*********************************************************************)
Nodes == A \cup B \cup C
ValDomain == 1..TMAX

LastTerm(log)  == IF Len(log)=0 THEN 0 ELSE log[Len(log)].term
LastIndex(log) == Len(log)

UpToDate(i, j) ==
  /\ (LastTerm(Log[i]) >  LastTerm(Log[j]))
   \/ (LastTerm(Log[i]) = LastTerm(Log[j]) /\ LastIndex(Log[i]) >= LastIndex(Log[j]))

\* A 域内包含 index i 的副本计数 > |A|/2
AHasMajorityAt(i) ==
  Cardinality({ n \in A : i <= Len(Log[n]) }) > Cardinality(A) \div 2

Min(a,b) == IF a <= b THEN a ELSE b

vars == << Alive, Term, VotedFor, Role, Leader, Log, CommitIndex, NextVal >>

(*********************************************************************)
(* 初始：受限域初始化                                                  *)
(*********************************************************************)
Init ==
  /\ Alive = Nodes
  /\ Term = [n \in Nodes |-> 0]
  /\ VotedFor = [n \in Nodes |-> NoLeader]
  /\ Log = [n \in Nodes |-> << >>]
  /\ CommitIndex = 0
  /\ NextVal \in ValDomain
  /\ NextVal = 1
  /\ \E l \in A:
       /\ Leader = l
       /\ Role = [n \in Nodes |-> IF n = l THEN "Leader" ELSE "Follower"]

(*********************************************************************)
(* 动作（已加“围栏”约束）                                              *)
(*********************************************************************)
ClientAppend ==
  /\ Leader # NoLeader
  /\ Role[Leader] = "Leader"
  /\ Len(Log[Leader]) < MAXLEN                     \* 日志长度上限
  /\ LET entry == [term |-> Term[Leader], val |-> NextVal] IN
     /\ Log' = [Log EXCEPT ![Leader] = Append(@, entry)]
     /\ NextVal' = IF NextVal = TMAX THEN 1 ELSE NextVal + 1   \* 值域循环
     /\ UNCHANGED << Alive, Term, VotedFor, Role, Leader, CommitIndex >>

ReplicateToA ==
  /\ Leader # NoLeader
  /\ \E n \in (A \cap Alive):
        /\ n # Leader
        /\ Len(Log[n]) < Len(Log[Leader])          \* 只在落后时复制
        /\ Log' = [Log EXCEPT ![n] = Append(@, Log[Leader][Len(Log[Leader])])]
        /\ UNCHANGED << Alive, Term, VotedFor, Role, Leader, CommitIndex, NextVal >>

\* 与 Leader 无关地推进提交：扫描到 MAXLEN
AdvanceCommit ==
  /\ \E i \in (CommitIndex+1)..MAXLEN:
        /\ AHasMajorityAt(i)
        /\ CommitIndex' = i
        /\ UNCHANGED << Alive, Term, VotedFor, Role, Leader, Log, NextVal >>

ReplicateToBC ==
  /\ REPL_BC = TRUE
  /\ Leader # NoLeader
  /\ \E n \in ((B \cup C) \cap Alive):
        /\ Len(Log[n]) < Len(Log[Leader])
        /\ LET k == Min(Len(Log[n]) + 1, Len(Log[Leader])) IN
           /\ Log' = [Log EXCEPT ![n] = SubSeq(Log[Leader], 1, k)]
        /\ UNCHANGED << Alive, Term, VotedFor, Role, Leader, CommitIndex, NextVal >>

Crash ==
  /\ \E n \in Alive:
       /\ Alive' = Alive \ {n}
       /\ UNCHANGED << Term, VotedFor, Role, Leader, Log, CommitIndex, NextVal >>

Recover ==
  /\ \E n \in (Nodes \ Alive):
       /\ Alive' = Alive \cup {n}
       /\ UNCHANGED << Term, VotedFor, Role, Leader, Log, CommitIndex, NextVal >>

StartElection ==
  /\ Leader' = NoLeader
  /\ \E c \in (A \cap Alive):
       /\ Role' = [Role EXCEPT ![c] = "Candidate"]
       /\ Term' = [i \in Nodes |-> IF i = c THEN Term[i] + 1 ELSE Term[i]]
       /\ \A i \in Nodes: Term'[i] <= TMAX            \* 任期上限
       /\ VotedFor' = [VotedFor EXCEPT ![c] = c]
  /\ UNCHANGED << Alive, Log, CommitIndex, NextVal >>

GrantVotes ==
  /\ \E c \in (A \cap Alive):
     /\ Role[c] = "Candidate"
     \* A 域活节点中，愿意/能投给 c 且 up-to-date 的节点数 > |A|/2
     /\ Cardinality({ v \in (A \cap Alive) :
            (VotedFor[v] = NoLeader \/ VotedFor[v] = c) /\ UpToDate(c, v) }) > Cardinality(A) \div 2
     /\ Leader' = c
     /\ Role'   = [Role EXCEPT ![c] = "Leader"]
     /\ VotedFor' = [n \in Nodes |-> IF n \in (A \cap Alive) THEN c ELSE VotedFor[n]]
     /\ UNCHANGED << Alive, Term, Log, CommitIndex, NextVal >>

NoLeaderWhenAMajorityDown ==
  /\ ~(Cardinality(Alive \cap A) > Cardinality(A) \div 2)
  /\ Leader' = NoLeader
  /\ UNCHANGED << Alive, Term, VotedFor, Role, Log, CommitIndex, NextVal >>

Next ==
  \/ ClientAppend
  \/ ReplicateToA
  \/ ReplicateToBC
  \/ AdvanceCommit
  \/ Crash
  \/ Recover
  \/ StartElection
  \/ GrantVotes
  \/ NoLeaderWhenAMajorityDown

Spec == Init /\ [][Next]_vars

(*********************************************************************)
(* 不变式                                                              *)
(*********************************************************************)
\* 提交在 A 域多数可见
CommittedOnAMajority ==
  \A i \in 1..CommitIndex:
     Cardinality({ n \in A : i <= Len(Log[n]) }) > Cardinality(A) \div 2

\* 新 leader（若存在）包含所有已提交日志
LeaderCompleteness ==
  (Leader = NoLeader) \/ (\A i \in 1..CommitIndex: i <= Len(Log[Leader]))

\* 提交索引不超过围栏
BoundedCommit ==
  CommitIndex <= MAXLEN

Safety == CommittedOnAMajority /\ LeaderCompleteness /\ BoundedCommit

======================================================================
