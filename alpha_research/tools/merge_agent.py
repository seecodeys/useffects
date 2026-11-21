"""Merge an agent's private registry (results/registry_agent_NNN.csv) into results/registry.csv and delete it."""
import os, sys
import pandas as pd
HERE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
for n in sys.argv[1:]:
    p = os.path.join(HERE, "results", f"registry_agent_{n}.csv")
    if not os.path.exists(p): print("missing", p); continue
    main = os.path.join(HERE, "results", "registry.csv")
    a = pd.read_csv(p); m = pd.read_csv(main)
    pd.concat([m, a], ignore_index=True).to_csv(main, index=False); os.remove(p)
    print(f"merged {len(a)} rows from agent {n}; registry now {len(m) + len(a)}")
