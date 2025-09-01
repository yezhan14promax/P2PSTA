package main

import (
	"fmt"
	"sort"
)

const m = 4
const ringSize = 1 << m

type Node struct {
	ID        int
	Finger    [m]*Node
	Successor *Node
}

var ring []*Node

// Simule un anneau trié
func insertNode(newNode *Node) {
	ring = append(ring, newNode)
	sort.Slice(ring, func(i, j int) bool {
		return ring[i].ID < ring[j].ID
	})
	updateSuccessors()
}

// Met à jour les successeurs de chaque nœud
func updateSuccessors() {
	n := len(ring)
	for i, node := range ring {
		node.Successor = ring[(i+1)%n]
	}
}

// Cherche le successeur de id dans l'anneau
func findSuccessor(id int) *Node {
	for _, node := range ring {
		if node.ID >= id {
			return node
		}
	}
	return ring[0] // wrap-around
}

// Met à jour la finger table du nœud
func (n *Node) updateFingerTable() {
	for i := 0; i < m; i++ {
		start := (n.ID + (1 << i)) % ringSize
		n.Finger[i] = findSuccessor(start)
	}
}

func (n *Node) printInfo() {
	fmt.Printf("Node %d (succ %d):\n", n.ID, n.Successor.ID)
	for i := 0; i < m; i++ {
		start := (n.ID + (1 << i)) % ringSize
		fmt.Printf("  Finger[%d]: start=%2d -> Node %d\n", i+1, start, n.Finger[i].ID)
	}
	fmt.Println()
}

func main() {
	// Étape 1 : créer les nœuds initiaux
	initialIDs := []int{1, 3, 7, 12}
	for _, id := range initialIDs {
		insertNode(&Node{ID: id})
	}
	for _, node := range ring {
		node.updateFingerTable()
	}

	fmt.Println("Avant ajout du nœud 5 :")
	for _, node := range ring {
		node.printInfo()
	}

	// Étape 2 : ajout du nœud 5
	node5 := &Node{ID: 5}
	insertNode(node5)
	for _, node := range ring {
		node.updateFingerTable()
	}

	fmt.Println("Après ajout du nœud 5 :")
	for _, node := range ring {
		node.printInfo()
	}
}
