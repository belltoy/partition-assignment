use std::collections::{BTreeMap, BTreeSet, HashMap};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Node(u16);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord)]
struct Partition(u16);

type Assignment = BTreeMap<Partition, Vec<Node>>;

macro_rules! debug {
    ($($e:expr),*) => {
        println!("    [DEBUG] {}", format!($($e),*));
    };
    ($fmt:expr, $($arg:expr),*) => {
        println!("    [DEBUG] {}", format!($fmt, $($arg),*));
    };
}

fn main() {
    let replication_factor = 3;
    let partitions = 60;
    let nodes: Vec<_> = (1..=8).map(|n| Node(n)).collect();

    let assignment = init(nodes, partitions, replication_factor);
    println!("\n==== Initialized Assignment: ====");
    print_partitions(&assignment, None);

    // Remove Node(5) and reassignment
    let remove = Node(2);
    let (assignment, moves) = remove_node(&assignment, &remove, replication_factor);
    println!("\n==== After {:?} removed: ====\nMoves: {}", remove, moves);
    print_partitions(&assignment, None);

    // Remove Node(4) and reassignment
    let remove = Node(4);
    let (assignment, moves) = remove_node(&assignment, &remove, replication_factor);
    println!("\n==== After {:?} removed: ====\nMoves: {}", remove, moves);
    print_partitions(&assignment, None);

    // Add a new Node(6) and reassignment
    // TODO: Add node

}

fn init(nodes: Vec<Node>, partitions: usize, replication_factor: usize)
    -> Assignment
{
    let n = nodes.into_iter().cycle()
        .take(partitions * replication_factor)
        .collect::<Vec<_>>();

    let assignment = n.windows(replication_factor)
        .take(partitions)
        .enumerate()
        .map(|(i, nodes)| (Partition(i as u16 + 1), Vec::from(nodes)))
        .collect();

    balance_boundary(assignment, 0).0
}

fn remove_node(
    assignment: &Assignment,
    remove: &Node,
    replication_factor: usize,
)
    -> (Assignment, usize)
{
    let mut moves = 0;

    if !assignment.iter().any(|(_p, ns)| ns.contains(&remove)) {
        panic!("Node {:?} is not contained in the assignment", remove);
    }

    let current_nodes = assignment.values().flatten().collect::<BTreeSet<_>>();
    if current_nodes.len() <= replication_factor {
        panic!("NO less nodes then the replication factor");
    }

    // partitions_on_remove: Partition => [Node]
    let partitions_on_remove = assignment
        .iter()
        .filter(|(_p, ns)| ns.contains(&remove))
        .map(|(p, ns)| {
            let ns = ns
                .iter()
                .filter(|&n| n != remove)
                .cloned()
                .collect::<Vec<_>>();
            (p.clone(), ns)
        })
        .collect::<BTreeMap<_, _>>();

    let mut remains: Assignment = assignment
        .iter()
        .map(|(p, ns)| {
            let ns = ns.into_iter().filter(|n| n != &remove).cloned().collect::<Vec<_>>();
            (p.clone(), ns.clone())
        })
        .collect();

    // transform remains to Node => Set(partitions)
    let mut remains_nodes: BTreeMap<Node, BTreeSet<_>> = Default::default();
    for (p, ns) in assignment {
        for n in ns {
            if n != remove {
                let v = remains_nodes.entry(n.clone()).or_default();
                v.insert(p.clone());
            }
        }
    }

    // Group by alternatives for these partitions
    let mut groups: HashMap<_, Vec<(_, _)>> = Default::default();

    // px: node => partitions(num) on remove
    let mut px: HashMap<_, Vec<_>> = Default::default();

    for (p, ns) in &partitions_on_remove {
        for n in ns {
            let v = px.entry(n).or_default();
            v.push(p.clone());
        }
    }

    let px = px.iter().map(|(n, ps)| (n, ps.len())).collect::<HashMap<_, _>>();

    for (p, ns) in &partitions_on_remove {
        let alters = remains_nodes
            .iter()
            .filter(|(_n, ps)| !ps.contains(&p))
            .map(|(n, ps)| (n.clone(), ps.len()))
            .collect::<Vec<_>>();
        let v = groups.entry(alters).or_default();
        v.push((p.clone(), ns.clone()));
    }

    let mut groups = groups.into_iter().collect::<Vec<_>>();
    groups.sort_by(|(ns1, _), (ns2, _)| {
        let sum_ns1 = ns1.iter().map(|(n, _)| px.get(&n).unwrap_or(&0)).sum::<usize>();
        let sum_ns2 = ns2.iter().map(|(n, _)| px.get(&n).unwrap_or(&0)).sum::<usize>();
        sum_ns2.cmp(&sum_ns1)
    });

    // debug!("groups len: {}", groups.len());
    while let Some((mut group_key, mut pps)) = groups.pop() {
        let mut f: Vec<(Partition, Vec<Node>)> = Default::default();
        while let Some((_p, _ns)) = pps.first() {
            // debug!("======== group_key {:?} ========", group_key);
            group_key.sort_by(|(_n1, len1), (_n2, len2)| {
                len1.cmp(&len2)
            });
            let upper = group_key.last().unwrap().1;
            let lower = group_key.first().unwrap().1;

            if upper == lower {
                // cycle
                let rest = group_key.iter().map(|(n, _len)| n).cycle().take(pps.len()).zip(pps).map(|(n, (p, ns))| {
                    let mut ns = ns.clone();
                    ns.push(n.clone());
                    moves += 1;
                    (p.clone(), ns)
                });

                f.extend(rest);

                // update rest groups
                remains = remains.into_iter()
                    .filter(|(_p, ns)| !ns.contains(remove))
                    .chain(f.into_iter()).collect();

                break;
            } else {
                let (p, ns) = pps.remove(0);
                // debug!(">>> pick: {:?}", group_key.first().unwrap());
                moves += 1;
                group_key.first_mut().unwrap().1 += 1;
                let mut ns = ns.clone();
                ns.push(group_key.first().unwrap().0.clone());

                // update rest groups
                remains = remains.into_iter()
                    .filter(|(_p, ns)| !ns.contains(remove))
                    .chain(Some((p.clone(), ns.clone())).into_iter()).collect();
            }
        }
        cal_groups(&remains, &mut groups);
    }

    // If upper bound - lower bound > 1, then need to reassign, just move a partition from
    // the node with the most partitions to the node with the least partitions.
    balance_boundary(remains, moves)
}

fn balance_boundary(mut assignment: Assignment, mut moves: usize) -> (Assignment, usize) {
    let mut nodes_map: HashMap<Node, Vec<&Partition>> = Default::default();
    for (p, ns) in &assignment {
        for n in ns {
            let v = nodes_map.entry(n.clone()).or_default();
            v.push(p);
        }
    }

    let mut nodes = nodes_map.iter().map(|(n, ps)| (n.clone(), ps.iter().cloned().cloned().collect::<Vec<_>>())).collect::<Vec<_>>();
    nodes.sort_by(|(_n1, ps1), (_n2, ps2)| {
        ps1.len().cmp(&ps2.len())
    });

    if nodes.last().unwrap().1.len() - nodes.first().unwrap().1.len() <= 1 {
        return (assignment, moves);
    }

    // find a partition on the upper bound node but the lower bound node doesn't have
    let upper = nodes.last().unwrap();
    let lower = nodes.first().unwrap();
    let n_ps = nodes_map.get(&upper.0).unwrap().iter().cloned().cloned().collect::<Vec<_>>();
    for p in n_ps.iter().rev() {
        if !lower.1.contains(&p) {
            // move p from upper to lower
            debug!("Move partition {} from upper bound node {} to lower bound node {}", p.0, upper.0.0, lower.0.0);
            assignment.entry(p.clone()).and_modify(|ns| {
                for n in ns {
                    if n.0 == upper.0.0 {
                        moves += 1;
                        *n = lower.0.clone();
                    }
                }
            });
            return balance_boundary(assignment, moves);
        }
    }

    (assignment, moves)
}

fn cal_groups(assignment: &Assignment, groups: &mut Vec<(Vec<(Node, usize)>, Vec<(Partition, Vec<Node>)>)>) {
    let mut remains_nodes: BTreeMap<Node, Vec<_>> = Default::default();
    for (p, ns) in assignment {
        for n in ns {
            let v = remains_nodes.entry(n.clone()).or_default();
            v.push(p.clone());
        }
    }

    for (group, _pps) in groups.iter_mut() {
        for (node, len) in group.iter_mut() {
            *len = remains_nodes.get(node).map(|ps| ps.len()).unwrap();
        }
    }
}

fn print_partitions<'a, I>(partitions: I, prefix: Option<&str>)
    where I: IntoIterator<Item = (&'a Partition, &'a Vec<Node>)> + Clone
{
    let prefix = prefix.unwrap_or("");

    let mut upper = usize::MIN;
    let mut lower = usize::MAX;

    println!("{prefix}Partition\tNodes");
    println!("{prefix}----------\t---------");
    for (p, ns) in partitions.clone() {
        println!("{prefix}       {:>2}\t{}",
        p.0,
        ns.iter().map(|n| format!("{:>2}", n.0)).collect::<Vec<_>>().join(", "));
    }

    let mut nodes: BTreeMap<&Node, Vec<&Partition>> = Default::default();
    for (p, ns) in partitions {
        for node in ns {
            let v = nodes.entry(node).or_default();
            v.push(p);
            v.sort();
        }
    }

    println!("{prefix}\n{prefix}Node\t Num\tPartitions");
    println!("{prefix}----\t----\t----------");
    for (n, ps) in &nodes {
        let ps_len = ps.len();
        if ps_len > upper {
            upper = ps_len;
        }
        if ps_len < lower {
            lower = ps_len;
        }

        println!("{prefix}{:>4}\t{:>4}\t{}",
          n.0,
          ps.len(),
          ps.iter()
            .map(|p| format!("{:>2}", p.0)).collect::<Vec<_>>().join(", "));
    }

    println!("{prefix}upper: {}, lower: {}, Differ: {}", upper, lower, upper - lower);
}
