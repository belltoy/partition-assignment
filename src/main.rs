use std::num::{NonZeroU32, NonZeroU8};
use std::collections::{BTreeMap, BTreeSet, HashMap};
use clap::{Parser, Subcommand};
use clap_stdin::FileOrStdin;
use serde::{Deserialize, Serialize};
use log::debug;
use anyhow::{anyhow, bail, Error, Result};

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Node(String);

#[derive(Debug, Clone, Hash, PartialEq, Eq, PartialOrd, Ord, Serialize, Deserialize)]
struct Partition(u32);

#[derive(Debug, Clone, Default, Serialize, Deserialize)]
struct Assignment(BTreeMap<Partition, Vec<Node>>);

#[derive(Debug, Parser)]
#[command(author, version, about, long_about = None)]
#[command(propagate_version = true)]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Debug, Clone, Deserialize, clap::ValueEnum)]
enum OutputFormat {
    Json,
    Text,
}

#[derive(Debug, Subcommand)]
enum Command {
    /// Initialize the assignment
    Init {
        /// The number of partitions
        #[arg(short, long, default_value = "60")]
        partitions: NonZeroU32,

        /// The replication factor
        #[arg(short, long, default_value = "3")]
        replication_factor: NonZeroU8,

        /// The nodes to assign these partitions to, in comma-separated format
        #[arg(short, long, value_delimiter = ',')]
        nodes: Vec<Node>,

        /// The output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat,

        /// Whether to include the actions list in the JSON output
        #[arg(short, long, default_value = "false")]
        with_actions: bool,
    },

    /// Add a node to the assignment, and reassign partitions
    Add {
        /// Node to add
        #[arg(short, long)]
        node: Node,

        /// The existing assignment file
        #[arg(short, long, default_value = "-")]
        input: FileOrStdin<Assignment>,

        /// Whether to include the actions list in the JSON output
        #[arg(short, long, default_value = "false")]
        with_actions: bool,

        /// The output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat,
    },

    /// Remove a node from the assignment, and reassign partitions
    Remove {
        /// Node to remove
        #[arg(short, long)]
        node: Node,

        /// The replication factor
        #[arg(short, long)]
        replication_factor: NonZeroU8,

        /// The existing assignment file
        #[arg(short, long, default_value = "-")]
        input: FileOrStdin<Assignment>,

        /// Whether to include the actions list in the JSON output
        #[arg(short, long, default_value = "false")]
        with_actions: bool,

        /// The output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat,
    },

    /// Validate the assignment
    Validate {
        /// The number of partitions
        #[arg(short, long)]
        partitions: NonZeroU32,

        /// The replication factor
        #[arg(short, long)]
        replication_factor: NonZeroU8,

        /// The existing assignment file
        #[arg(short, long, default_value = "-")]
        input: FileOrStdin<Assignment>,

        /// The output format
        #[arg(short, long, value_enum, default_value_t = OutputFormat::Text)]
        output_format: OutputFormat,
    },
}

#[derive(Debug, Default, Serialize)]
struct Output {
    assignment: Assignment,

    moves: Vec<Move>,

    moves_count: usize,
}

#[derive(Debug, Clone, Serialize)]
struct Move {
    partition: Partition,
    from: Node,
    to: Node,
}

fn main() -> Result<()> {
    pretty_env_logger::init();
    let cli = Cli::parse();
    cli.command.exec()?;

    Ok(())
}

fn init(nodes: &[Node], partitions: usize, replication_factor: usize) -> Assignment {
    let n = nodes.iter().cycle()
        .take(partitions * replication_factor)
        .cloned()
        .collect::<Vec<_>>();

    let assignment = n.windows(replication_factor)
        .take(partitions)
        .enumerate()
        .map(|(i, nodes)| (Partition(i as u32 + 1), Vec::from(nodes)))
        .collect::<Assignment>();

    let nodes_map = assignment.nodes_map();
    balance_boundary(assignment, nodes_map, vec![], 0).0
}

fn add_node(assignment: Assignment, add: Node) -> Result<(Assignment, Vec<Move>, usize)> {
    let mut nodes_map = assignment.nodes_map();
    let moves = vec![];
    nodes_map.entry(add.clone()).or_default();
    Ok(balance_boundary(assignment, nodes_map, moves, 0))
}

fn remove_node(assignment: &Assignment, remove: &Node, replication_factor: usize)
    -> Result<(Assignment, Vec<Move>, usize)>
{
    let mut moves_count = 0;
    let mut moves = vec![];

    if !assignment.0.iter().any(|(_p, ns)| ns.contains(&remove)) {
        bail!("{remove} is not contained in the assignment");
    }

    let current_nodes = assignment.0.values().flatten().collect::<BTreeSet<_>>();
    if current_nodes.len() <= replication_factor {
        bail!("NO less nodes then the replication factor");
    }

    // partitions_on_remove: Partition => [Node]
    let partitions_on_remove = assignment.0
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

    let mut remains: Assignment = assignment.0
        .iter()
        .map(|(p, ns)| {
            let ns = ns.into_iter().filter(|n| n != &remove).cloned().collect::<Vec<_>>();
            (p.clone(), ns.clone())
        })
        .collect();

    // transform remains to Node => Set(partitions)
    let mut remains_nodes: BTreeMap<Node, BTreeSet<_>> = Default::default();
    for (p, ns) in &assignment.0 {
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

    debug!("groups len: {}", groups.len());
    while let Some((mut group_key, mut pps)) = groups.pop() {
        let mut f: Vec<(Partition, Vec<Node>)> = Default::default();
        while let Some((_p, _ns)) = pps.first() {
            debug!("======== group_key {:?} ========", group_key);

            group_key.sort_by(|(_n1, len1), (_n2, len2)| {
                len1.cmp(&len2)
            });
            let upper = group_key.last().unwrap().1;
            let lower = group_key.first().unwrap().1;

            if upper == lower {
                // cycle
                let rest = group_key
                    .iter()
                    .map(|(n, _len)| n)
                    .cycle()
                    .take(pps.len())
                    .zip(pps)
                    .map(|(n, (p, ns))| {
                        let mut ns = ns.clone();
                        ns.push(n.clone());
                        moves_count += 1;
                        moves.push(Move {
                            partition: p.clone(),
                            from: remove.clone(),
                            to: n.clone(),
                        });
                        (p.clone(), ns)
                    });

                f.extend(rest);

                // update rest groups
                remains = remains.0.into_iter()
                    .filter(|(_p, ns)| !ns.contains(remove))
                    .chain(f.into_iter()).collect();

                break;
            } else {
                let (p, ns) = pps.remove(0);
                debug!(">>> pick: {:?}", group_key.first().unwrap());
                moves_count += 1;
                moves.push(Move {
                    partition: p.clone(),
                    from: remove.clone(),
                    to: group_key.first().unwrap().0.clone(),
                });
                group_key.first_mut().unwrap().1 += 1;
                let mut ns = ns.clone();
                ns.push(group_key.first().unwrap().0.clone());

                // update rest groups
                remains = remains.0.into_iter()
                    .filter(|(_p, ns)| !ns.contains(remove))
                    .chain(Some((p.clone(), ns.clone())).into_iter()).collect();
            }
        }
        cal_groups(&remains, &mut groups);
    }

    // If upper bound - lower bound > 1, then need to reassign, just move a partition from
    // the node with the most partitions to the node with the least partitions.
    let nodes_map = remains.nodes_map();
    Ok(balance_boundary(remains, nodes_map, moves, moves_count))
}

fn balance_boundary(
    mut assignment: Assignment,
    nodes_map: HashMap<Node, Vec<Partition>>,
    mut moves: Vec<Move>,
    mut moves_count: usize
) -> (Assignment, Vec<Move>, usize) {
    let mut nodes = nodes_map.iter().map(|(n, ps)| (n.clone(), ps.iter().cloned().collect::<Vec<_>>())).collect::<Vec<_>>();
    nodes.sort_by(|(_n1, ps1), (_n2, ps2)| {
        ps1.len().cmp(&ps2.len())
    });

    if nodes.last().unwrap().1.len() - nodes.first().unwrap().1.len() <= 1 {
        return (assignment, moves, moves_count);
    }

    // find a partition on the upper bound node but the lower bound node doesn't have
    let upper = nodes.last().unwrap();
    let lower = nodes.first().unwrap();
    let n_ps = nodes_map.get(&upper.0).unwrap().iter().cloned().collect::<Vec<_>>();
    for p in n_ps.iter().rev() {
        if !lower.1.contains(&p) {
            // move p from upper to lower
            debug!("Move {p} from upper bound node {} to lower bound node {}", upper.0.0, lower.0.0);
            assignment.0.entry(p.clone()).and_modify(|ns| {
                for n in ns {
                    if n.0 == upper.0.0 {
                        moves_count += 1;
                        moves.push(Move {
                            partition: p.clone(),
                            from: upper.0.clone(),
                            to: lower.0.clone(),
                        });
                        *n = lower.0.clone();
                    }
                }
            });
            let nodes_map = assignment.nodes_map();
            return balance_boundary(assignment, nodes_map, moves, moves_count);
        }
    }

    (assignment, moves, moves_count)
}

fn cal_groups(assignment: &Assignment, groups: &mut Vec<(Vec<(Node, usize)>, Vec<(Partition, Vec<Node>)>)>) {
    let mut remains_nodes: BTreeMap<Node, Vec<_>> = Default::default();
    for (p, ns) in &assignment.0 {
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

    println!("{prefix}");
    println!("{prefix}upper: {upper}, lower: {lower}, Differ: {}", upper - lower);
}

fn print_moves(moves: &[Move]) {
    println!("==== Moves: ====");
    let mut sorted = moves.iter().collect::<Vec<_>>();
    sorted.sort_by(|m1, m2| {
        m1.partition.cmp(&m2.partition)
    });
    for m in sorted {
        println!("Move {} from {} to {}", m.partition, m.from, m.to);
    }
}

impl Command {
    fn exec(self) -> Result<()> {
        match self {
            Self::Init { partitions, replication_factor, nodes, output_format, with_actions } => {
                let replication_factor = replication_factor.get() as usize;
                if partitions.get() == 0 {
                    bail!("Partitions must not be zero");
                }

                if nodes.is_empty() {
                    bail!("Nodes must not be empty");
                }

                if nodes.len() < replication_factor {
                    bail!("Nodes must be greater than or equal to replication factor");
                }

                let assignment = init(&nodes[..], partitions.get() as usize, replication_factor);

                match output_format {
                    OutputFormat::Json => {
                        if with_actions {
                            let out = Output { assignment, ..Default::default() };
                            println!("{}", serde_json::to_string_pretty(&out)?);
                        } else {
                            println!("{}", serde_json::to_string_pretty(&assignment)?);
                        }
                    }
                    OutputFormat::Text => {
                        println!("==== Initialized Assignment: ====");
                        assignment.print();
                    }
                }
            }
            Self::Add { node, input, output_format, with_actions } => {
                let assignment = input.contents()?;
                if assignment.nodes_map().get(&node).is_some() {
                    bail!("{node} already exists in the assignment");
                }

                let (assignment, moves, moves_count) = add_node(assignment, node.clone())?;
                assert!(moves.len() == moves_count);

                match output_format {
                    OutputFormat::Json => {
                        if with_actions {
                            let out = Output {
                                assignment,
                                moves,
                                moves_count,
                            };
                            println!("{}", serde_json::to_string_pretty(&out)?);
                        } else {
                            println!("{}", serde_json::to_string_pretty(&assignment)?);
                        }
                    }
                    OutputFormat::Text => {
                        println!("==== After add node: {}, Assignment: ====", &node.0);
                        assignment.print();
                        println!("Moves: {}", moves_count);
                        print_moves(&moves);
                    }
                }
            }
            Self::Remove { node, input, replication_factor, with_actions, output_format } => {
                let assignment = input.contents()?;
                let replication_factor = replication_factor.get() as usize;
                assignment.validate(replication_factor)?;
                assignment.ensure_contains_node(&node)?;

                let partitions_on_remove = assignment.nodes_map().remove(&node).unwrap();
                let (assignment, moves, moves_count) = remove_node(&assignment, &node, replication_factor)?;
                assert!(moves.len() == moves_count);
                match output_format {
                    OutputFormat::Json => {
                        if with_actions {
                            let out = Output {
                                assignment,
                                moves,
                                moves_count,
                            };
                            println!("{}", serde_json::to_string_pretty(&out)?);
                        } else {
                            println!("{}", serde_json::to_string_pretty(&assignment)?);
                        }
                    }
                    OutputFormat::Text => {
                        println!("==== After remove node: {}, Assignment: ====", &node.0);
                        assignment.print();
                        println!("Moves: {}", moves_count);
                        println!("Removed node: {node}, partitions: [{}]", partitions_on_remove.iter().map(|p| format!("{}", p.0)).collect::<Vec<_>>().join(", "));
                        print_moves(&moves);
                    }
                }
            }
            Self::Validate { input, partitions, replication_factor, output_format } => {
                let partitions = partitions.get() as usize;
                let factor = replication_factor.get() as usize;
                let assignment = input.contents()?;

                for p in (1..=partitions as u32).map(From::from) {
                    assignment.0.get(&p).ok_or_else(|| {
                        anyhow!("{p} is missing")
                    })
                    .and_then(|ns| {
                        let nodes_num = ns.len();
                        if nodes_num != factor {
                            bail!("{p} replicas on {nodes_num} nodes, but replication factor is {factor}");
                        }
                        let mut ns1 = ns.clone();
                        ns1.dedup();

                        if ns1.len() != nodes_num {
                            let nodes_str = ns
                                    .iter()
                                    .map(|n| n.0.as_str())
                                    .collect::<Vec<_>>()
                                    .join(", ");
                            bail!("{p} replicas on duplicate nodes: {nodes_str}");
                        }
                        Ok(())
                    })?;

                    let nodes_map = assignment.nodes_map();
                    let nodes_num = nodes_map.len();
                    let avg = (partitions * factor) as f64 / nodes_num as f64;
                    let expect_lower = avg.floor() as usize;
                    let expect_upper = avg.ceil() as usize;
                    let expect = if expect_lower == expect_upper {
                        format!("{}", expect_lower)
                    } else {
                        format!("[{}, {}]", expect_lower, expect_upper)
                    };
                    for (n, ps) in &nodes_map {
                        let ps_num = ps.len();
                        if ps_num < expect_lower || ps_num > expect_upper {
                            bail!("{n} has {ps_num} partitions, but the balance number of partitions is {expect}");
                        }
                    }
                }

                match output_format {
                    OutputFormat::Json => {
                        println!("{}", serde_json::to_string_pretty(&assignment)?);
                    }
                    OutputFormat::Text => {
                        assignment.print();
                    }
                }
            }
        }

        Ok(())
    }
}

impl<S: AsRef<str>> From<S> for Node {
    fn from(s: S) -> Self {
        Self(s.as_ref().to_string())
    }
}

impl std::fmt::Display for Node {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Node({})", self.0)
    }
}

impl std::fmt::Display for Partition {
    fn fmt(&self, f: &mut std::fmt::Formatter) -> std::fmt::Result {
        write!(f, "Partition({})", self.0)
    }
}

impl<N: Into<u32>> From<N> for Partition {
    fn from(n: N) -> Self {
        Self(n.into())
    }
}

impl FromIterator<(Partition, Vec<Node>)> for Assignment {
    fn from_iter<T: IntoIterator<Item = (Partition, Vec<Node>)>>(iter: T) -> Self {
        Self(iter.into_iter().collect())
    }
}

impl std::str::FromStr for Assignment {
    type Err = Error;

    fn from_str(s: &str) -> Result<Self, Self::Err> {
        let assignment: Assignment = serde_json::from_str(s)?;
        Ok(assignment)
    }
}

impl Assignment {
    fn validate(&self, replication_factor: usize) -> Result<()> {
        if self.0.is_empty() {
            bail!("Assignment must not be empty");
        }

        let nodes = self.0.values().flatten().collect::<BTreeSet<_>>();
        if nodes.len() < replication_factor {
            bail!("Nodes must be greater than or equal to replication factor");
        }

        Ok(())
    }

    fn contains_node(&self, node: &Node) -> bool {
        self.0.iter().any(|(_p, ns)| ns.contains(node))
    }

    fn ensure_contains_node(&self, node: &Node) -> Result<()> {
        if !self.contains_node(node) {
            bail!("{node} is not contained in the assignment");
        }

        Ok(())
    }

    fn print(&self) {
        print_partitions(&self.0, None);
    }

    fn nodes_map(&self) -> HashMap<Node, Vec<Partition>> {
        let mut nodes_map: HashMap<Node, Vec<Partition>> = Default::default();
        for (p, ns) in &self.0 {
            for n in ns {
                let v = nodes_map.entry(n.clone()).or_default();
                v.push(p.clone());
            }
        }

        nodes_map
    }
}
