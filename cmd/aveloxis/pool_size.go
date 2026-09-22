// SPDX-FileCopyrightText: 2026 Sean Goggins, University of Missouri, Derek Howard
// SPDX-License-Identifier: MIT

package main

import (
	"context"
	"log/slog"

	"github.com/aveloxis/aveloxis/internal/config"
	"github.com/aveloxis/aveloxis/internal/db"
	"github.com/aveloxis/aveloxis/internal/mailinglist"
	"github.com/aveloxis/aveloxis/internal/scheduler"
)

// siblingProcessPools is how many other aveloxis processes share the
// server with serve on a standard deployment (`aveloxis start all`: web
// and api), each holding db.DefaultPoolMaxConns.
const siblingProcessPools = 2

// poolSizing is serve's connection-pool decision with its derivation, so
// the startup log carries the EFFECTIVE value and every number behind it
// (SR-10; feedback: a recommendation needs its denominator).
type poolSizing struct {
	Size       int32
	Demand     int
	Budget     int // 0 when the server could not be asked
	Override   int // database.pool_max_conns, 0 when unset
	Source     string
	Attributes []any
}

// servePoolSize picks the pool ceiling. An explicit database.pool_max_conns
// wins. Otherwise the scheduler's demand (scheduler.PoolDemand) is capped
// by the server's budget: max_connections minus the superuser reserve
// minus the sibling web/api pools. With no budget (the probe failed) the
// demand stands alone. A derived size never drops below the non-serve
// default (the floor the old workers+15 rule had); an explicit override is
// honored as written.
func servePoolSize(override, demand, serverMax, reserved int) poolSizing {
	ps := poolSizing{Demand: demand, Override: override}
	budget := 0
	if serverMax > 0 {
		budget = serverMax - reserved - siblingProcessPools*db.DefaultPoolMaxConns
		ps.Budget = budget
	}
	size := demand
	ps.Source = "demand"
	switch {
	case override > 0:
		size = override
		ps.Source = "database.pool_max_conns"
	case serverMax > 0 && demand > budget:
		// The server answered and cannot fund the demand. A budget at or
		// below zero (a tiny max_connections, or a reserve larger than it)
		// still caps — leaving the pool at demand would open connections
		// the server refuses at runtime, worse than the old workers+15
		// (review round 4). One connection is the least a pool can be.
		size = max(1, budget)
		ps.Source = "server budget (max_connections - reserve - web/api pools)"
	}
	// The floor applies to DERIVED sizes only: an explicit
	// database.pool_max_conns is the operator's throttle and is honored
	// as written (pgx needs at least one connection).
	switch {
	case override > 0 && size < 1:
		size = 1
	case override == 0 && size < db.DefaultPoolMaxConns && !(serverMax > 0 && budget < db.DefaultPoolMaxConns):
		// A derived size rises to the non-serve default — unless the
		// server's own budget is what kept it low; the floor may not
		// overrule the server.
		size = db.DefaultPoolMaxConns
		ps.Source += ", raised to the default floor"
	}
	ps.Size = int32(size)
	ps.Attributes = []any{
		"pool_size", ps.Size, "pool_source", ps.Source,
		"pool_demand", demand, "pool_override", override,
		"server_max_connections", serverMax, "server_reserved", reserved,
		"sibling_pools", siblingProcessPools * db.DefaultPoolMaxConns, "server_budget", budget,
	}
	return ps
}

// decideServePool computes and LOGS serve's pool size: demand from the
// scheduler's registry, the budget from the server, the override from
// config. A pool below demand is logged as a deliberate throttle so the
// operator knows the health probe's "pool exhausted" is expected, not an
// outage.
func decideServePool(ctx context.Context, cfg *config.Config, workers int, logger *slog.Logger) poolSizing {
	systems := 0
	if cfg.Collection.MailingListEnabled {
		if sys, err := mailinglist.LoadSystems(); err != nil {
			logger.Warn("mailing-list systems could not be loaded for the pool demand — counted as none", "error", err)
		} else {
			systems = len(sys)
		}
	}
	demand, demandAttrs := scheduler.PoolDemand(cfg, workers, systems)
	serverMax, reserved, err := db.ServerConnectionBudget(ctx, cfg.Database.ConnectionString())
	if err != nil {
		logger.Warn("server connection budget unavailable — sizing the pool from demand alone", "error", err)
		serverMax, reserved = 0, 0
	}
	ps := servePoolSize(cfg.Database.PoolMaxConns, demand, serverMax, reserved)
	logger.Info("database connection pool sized", append(append([]any{}, ps.Attributes...), demandAttrs...)...)
	if serverMax > 0 && ps.Override == 0 && ps.Budget < db.DefaultPoolMaxConns {
		logger.Warn("the server cannot fund even the default pool — raise max_connections",
			"server_max_connections", serverMax, "server_reserved", reserved, "server_budget", ps.Budget, "pool_size", ps.Size)
	}
	if int(ps.Size) < demand {
		logger.Warn("connection pool is below the scheduler's demand — workers will wait for connections at peak; the health probe then reports the pool exhausted, not the database down",
			"pool_size", ps.Size, "pool_demand", demand, "pool_source", ps.Source,
			"remedy", "raise max_connections (and database.pool_max_conns) or lower --workers; on a disk-bound server the throttle may be the right choice")
	}
	return ps
}
