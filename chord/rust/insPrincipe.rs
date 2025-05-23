use std::collections::BTreeMap;
use std::fmt;

const M: u32 = 4;
const RING_SIZE: u32 = 1 << M;

#[derive(Clone)]
struct Node {
    id: u32,
    finger: Vec<u32>, // identifiants des noeuds pointés
    successor: u32,
}

impl Node {
    fn new(id: u32) -> Self {
        Node {
            id,
            finger: vec![0; M as usize],
            successor: 0,
        }
    }

    fn update_finger_table(&mut self, ring: &Vec<u32>) {
        for i in 0..M {
            let start = (self.id + (1 << i)) % RING_SIZE;
            self.finger[i as usize] = find_successor(start, ring);
        }
    }

    fn print_info(&self) {
        println!("Node {} (succ {}):", self.id, self.successor);
        for (i, &f) in self.finger.iter().enumerate() {
            let start = (self.id + (1 << i)) % RING_SIZE;
            println!("  Finger[{}]: start={:2} -> Node {}", i + 1, start, f);
        }
        println!();
    }
}

fn find_successor(id: u32, ring: &Vec<u32>) -> u32 {
    for &node_id in ring {
        if node_id >= id {
            return node_id;
        }
    }
    ring[0] // wrap-around
}

fn update_successors(nodes: &mut BTreeMap<u32, Node>) {
    let ids: Vec<u32> = nodes.keys().cloned().collect();
    for (i, &id) in ids.iter().enumerate() {
        let next_id = ids[(i + 1) % ids.len()];
        nodes.get_mut(&id).unwrap().successor = next_id;
    }
}

fn main() {
    let mut nodes: BTreeMap<u32, Node> = BTreeMap::new();
    let mut ring_ids = vec![1, 3, 7, 12];

    // Étape 1: initialisation
    for &id in &ring_ids {
        nodes.insert(id, Node::new(id));
    }
    update_successors(&mut nodes);
    for node in nodes.values_mut() {
        node.update_finger_table(&ring_ids);
    }

    println!("Avant ajout du noeud 5:");
    for node in nodes.values() {
        node.print_info();
    }

    // Étape 2: ajout du noeud 5
    let new_id = 5;
    ring_ids.push(new_id);
    ring_ids.sort();
    nodes.insert(new_id, Node::new(new_id));
    update_successors(&mut nodes);
    for node in nodes.values_mut() {
        node.update_finger_table(&ring_ids);
    }

    println!("Après ajout du noeud 5:");
    for node in nodes.values() {
        node.print_info();
    }
}
