workspace "Rovqet" "Reference C4 model for the architecture-first research broker" {
  !identifiers hierarchical

  model {
    publisher = person "Publisher" "Sensor, controller or application publishing messages"
    subscriber = person "Subscriber" "Live or durable consumer, worker or actuator"
    operator = person "Experiment operator" "Runs test rig and interprets evidence"

    rovqet = softwareSystem "Rovqet" "Selective reliability broker for dense client connections" {
      sdk = container "Rust SDK" "Native client for publish, subscribe, ACK, retry and reconnect" "Rust"
      broker = container "Broker node" "One runtime process, replicated per cluster node" "Rust / Tokio" {
        transport = component "Session transport" "Bounded decoder, state machine, socket writer" "Tokio TCP"
        admission = component "Admission and scheduler" "Hierarchical budgets and priority queues" "Rust"
        router = component "Topic router" "Partition hash, subscriptions, ownership cache" "Rust"
        volatile = component "Q0 dispatcher" "Best-effort live fan-out" "Rust"
        q1 = component "Q1 partition actor" "Local fsync-backed durable log and replay" "Rust"
        q2 = component "Q2 Raft shard" "Durable majority replicated log" "Rust/OpenRaft"
        consumers = component "Durable consumer groups" "Leases, ACK, cursor, DLQ dispositions" "Rust"
        control = component "Metadata and ownership" "Snapshots, epoch fencing and placement" "Rust/Raft"
        telemetry = component "Measurement" "Counters and bounded trace export" "Rust"
      }
      localWal = container "Node WAL and snapshot files" "Per-node Q1 and Q2 data, checksums and manifests" "Filesystem / NVMe"
      rig = container "Benchmark and oracle" "Independent durable ACK/replay ledger and fault injector" "Rust/Python test tools"
    }
    baselines = softwareSystem "Reference brokers" "NATS Core/JetStream and MQTT baseline test targets"

    publisher -> sdk "Publishes Q0/Q1/Q2 messages"
    subscriber -> sdk "Subscribes, fetches, ACKs and replays"
    operator -> rig "Runs experiments and examines evidence"
    sdk -> transport "Native frames"
    transport -> admission "Validated requests"
    admission -> router "Admitted request"
    router -> volatile "Q0 route and recipient set"
    router -> q1 "Q1 local partition route"
    router -> q2 "Q2 leader route"
    q1 -> localWal "Durable local WAL and recovery"
    q2 -> localWal "Durable replicated entries and snapshots"
    q1 -> consumers "Committed Q1 records and state"
    q2 -> consumers "Committed Q2 records and state"
    control -> router "Versioned ownership snapshot"
    control -> q2 "Authorized replica membership"
    volatile -> transport "Bounded Q0 deliveries"
    consumers -> transport "Durable deliveries and ACK outcomes"
    admission -> telemetry "Resource and rejection counters"
    rig -> broker "Stress and fault control"
    rig -> baselines "Comparable reference workload"

    research = deploymentEnvironment "Research Lab" {
      host1 = deploymentNode "Host 1" "Broker + local data" "Linux" {
        containerInstance broker
        containerInstance localWal
      }
      host2 = deploymentNode "Host 2" "Broker + local data" "Linux" {
        containerInstance broker
        containerInstance localWal
      }
      host3 = deploymentNode "Host 3" "Broker + local data" "Linux" {
        containerInstance broker
        containerInstance localWal
      }
    }
  }

  views {
    systemContext rovqet "SystemContext" {
      include *
      autoLayout lr
    }
    container rovqet "Containers" {
      include *
      autoLayout lr
    }
    component rovqet.broker "BrokerComponents" {
      include *
      autoLayout lr
    }
    deployment rovqet research "ResearchDeployment" {
      include *
      autoLayout lr
    }
    styles {
      element "Software System" {
        background #23415C
        color #ffffff
      }
      element "Container" {
        background #376A87
        color #ffffff
      }
      element "Component" {
        background #DDEAF0
        color #18232C
      }
      element "Person" {
        shape person
        background #536E7A
        color #ffffff
      }
    }
  }
}
