package dbresolver

import "database/sql"

// New creates a new database resolver with the given options. At least one
// primary database must be configured via WithPrimaryDBs.
//
// Returns ErrNoPrimaryDB if no primaries are provided.
func New(opts ...OptionFunc) (DB, error) {
	opt := &option{
		lbPolicy: RoundRobinLB,
	}
	for _, fn := range opts {
		fn(opt)
	}

	if len(opt.primaries) == 0 {
		return nil, ErrNoPrimaryDB
	}

	analyzer := opt.queryAnalyzer
	if analyzer == nil {
		analyzer = &defaultQueryAnalyzer{}
	}

	primaryLB := opt.customLB
	if primaryLB == nil {
		primaryLB = newLoadBalancer(opt.lbPolicy)
	}
	primaryPool := &nodePool{
		nodes: opt.primaries,
		lb:    primaryLB,
	}

	var replicaPool *nodePool
	if len(opt.replicas) > 0 {
		replicaLB := opt.customLB
		if replicaLB == nil {
			replicaLB = newLoadBalancer(opt.lbPolicy)
		}
		replicaPool = &nodePool{
			nodes: opt.replicas,
			lb:    replicaLB,
		}
	}

	if opt.primaryPool != nil {
		applyPoolConfig(opt.primaries, opt.primaryPool)
	}
	if opt.replicaPool != nil {
		applyPoolConfig(opt.replicas, opt.replicaPool)
	}

	r := &resolver{
		primaryPool: primaryPool,
		replicaPool: replicaPool,
		analyzer:    analyzer,
	}

	if opt.healthCheck.enable {
		hc := newHealthChecker(
			opt.healthCheck.interval,
			opt.healthCheck.failThreshold,
			opt.healthCheck.recoverThreshold,
		)
		allNodes := make([]*Node, 0, len(opt.primaries)+len(opt.replicas))
		allNodes = append(allNodes, opt.primaries...)
		allNodes = append(allNodes, opt.replicas...)
		hc.register(allNodes)

		primaryPool.checker = hc
		if replicaPool != nil {
			replicaPool.checker = hc
		}
		r.healthChecker = hc
		hc.start()
	}

	return r, nil
}

func applyPoolConfig(nodes []*Node, cfg *PoolConfig) {
	for _, node := range nodes {
		applyPoolConfigToDB(node.db, cfg)
	}
}

func applyPoolConfigToDB(db *sql.DB, cfg *PoolConfig) {
	if cfg.MaxOpenConns > 0 {
		db.SetMaxOpenConns(cfg.MaxOpenConns)
	}
	if cfg.MaxIdleConns > 0 {
		db.SetMaxIdleConns(cfg.MaxIdleConns)
	}
	if cfg.ConnMaxLifetime > 0 {
		db.SetConnMaxLifetime(cfg.ConnMaxLifetime)
	}
	if cfg.ConnMaxIdleTime > 0 {
		db.SetConnMaxIdleTime(cfg.ConnMaxIdleTime)
	}
}
