# Example only — placeholders, not for production deploy.

job "mysql-guard" {
  namespace = "mysql"

  group "db" {
    count = 2

    constraint {
      attribute = "${node.unique.name}"
      operator  = "regexp"
      value     = "^db-(a|b)$"
    }

    spread {
      attribute = "${node.unique.id}"
    }

    distinct_hosts = true

    task "mysql-guard" {
      driver = "docker"

      env {
        GUARD_ROLE    = "db"
        GUARD_NODE    = "${node.unique.name}"
        GUARD_MODE    = "observe"
        GUARD_CLUSTER = "example-cluster"
        GUARD_PEERS   = "db-a,db-b"
      }

      config {
        image = "ghcr.io/example/mysql-guard:latest"
        args = [
          "--node=${node.unique.name}",
          "--role=db",
          "--mode=observe",
          "--cluster=example-cluster",
          "--peers=db-a,db-b",
          "--consul-addr=${NOMAD_IP_http}:8500",
        ]
      }

      resources {
        cpu    = 200
        memory = 256
      }

      service {
        name = "mysql-guard"
        port = "metrics"
        tags = ["mysql-guard"]

        check {
          type     = "http"
          path     = "/healthz"
          interval = "10s"
          timeout  = "2s"
        }
      }
    }
  }

  group "witness" {
    count = 1

    constraint {
      attribute = "${node.unique.name}"
      operator  = "regexp"
      value     = "^witness-"
    }

    task "mysql-guard" {
      driver = "docker"

      env {
        GUARD_ROLE    = "witness"
        GUARD_MODE    = "observe"
        GUARD_CLUSTER = "example-cluster"
        GUARD_PEERS   = "db-a,db-b"
      }

      config {
        image = "ghcr.io/example/mysql-guard:latest"
        args = [
          "--node=guard-${node.unique.name}",
          "--role=witness",
          "--cluster=example-cluster",
          "--peers=db-a,db-b",
        ]
      }
    }
  }
}
