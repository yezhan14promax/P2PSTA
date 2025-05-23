

# Vision synthèse des réseaux P2P

## Une DHT-clé ou réseau construit suivant un Graph - Tree (kd-tree, semantique, ...)

 - Une DHT-clé

   * Comment construire une clé:
       - Range (ordonne l'anneau)
       - Spatial, utilise des clés comme hilbert, etc

   * Routage en log de N

 - Un Graph
   * Les arcs = critères de routage
   * Routage: Suit les arcs
     - Range, equivaut au principe du B-Tree
     - Spatial, utilise des clés comme hilbert, etc
     - Pour optimiser le routage, des travaux proposes de propager une méta-description des contenus des noeuds. 
     - Sans optimisation: Routage en log de N
   * Les noeuds pointent vers des noeuds de stockages comme IFS, Chord, ... Besoin d'une organisation pour *Trouver les noeuds* 
 


## Modèles Mono-Attribut vs Modèles Multi-Attributs
 * En Mono

  - Une DHT-clé ou Graph (kd-tree, semantique, ...) aboutit aux noeuds qui stocke la donnée

 * En Multi (comment traiter la conjonction des prédicats)

  Une DHT-clé ou Graph (kd-tree, semantique, ...) aboutit pour chaque valeur d'attribut

  * à un identifiant clé [MANN][SWORD]
      - Requête = résolution par intersection des noeuds.
          ** Cas de MANN, doit au final aller chercher la donnée
    - pro:
        - stockage efficace et mise à jour efficace
    - cons:
        - mise à jour necessite un hop de plus
        - requête demande plusieurs hops

  * au stockage complet de la donnée.
[Mercury]
      - Requête = choisit celui qui est le plus discriminant, envoie à ce noeud qui ensuite applique les prédicats sur les autres attributs
      - pro:
          - Très efficace en hop

      - cons:
          - Volume des données
          - Mise à jour 

NOTE: En terme de tuning, utilise soit des index primaires, soit des index secondaires mais pas un mixe des deux

NOTE: Tous les attributs doivent être indéxés

## Fonctionnement du réseau

 * Load Balancing
   - En DHT-clé:
   	Protocole Chord, Dynamo, le fait 

   - En Graph 
      - Solution: Des grappes de noeuds, matrix, ...

 * Persistance/Resiliance
   - En DHT-clé:
   	Protocole Chord, Dynamo, le fait 

   - En Graph 
      - Solution: Mécanisme comme les chiens de gardes. Protocole couteux et pas prouvé que c'est fiable. exemple si plusieurs noeuds se perdent, comment maintenir ?



[SWORD] Design and implementation trade-offs for wide-area resource discovery

[Maan] Maan: A multi-attribute addressable network for grid information services

[Mercury] Mercury: supporting scalable multi-attribute range queries


# Idée à discuter

Comment indexer des trajectoires ?


## Problème:

Contribution, Index non dense pour les trajectoires

Un objet trajectoire contient beaucoup de points vs un objet spatio temporel.

Conséquence: indexer tous ces points devient prohibitives.

Les modèles sont ST mais pour des points et non des trajectoires.


## Propositions:

 Un modèle en versionning et non ponctuel
 Un index *non denses* basé sur un découpage de la clé en unité discrete
 Basé sur une extension de Dynamo pour le modèle multiattribut
 Une espace d'addressage de Chord en range

 Des super-peers qui rassemblent les morceaux de trajectoires

## Principe du modèle:

Une tesselation de l'espace et du temps en cellules { C }

Z = trajectoire ST

R = Un objet STA (A1,...An)

{ I } = Z INTO C ; I est l'identifiant des cellules qui forme une clé

{ Ic } = Z INTERSECT C ; Ic ce sont des morceaux de trajectoires

Nodes(I) <- R

Indexer chaque colonne independament. (IdObj, IdAtt(Ai), Ts, Val) -> Dynamo
   
 * Sur le noeud pour la clé: IdObj || IdAtt || J(Ts)  J est une décomposition en intervalle du temps


Mais pour le spatial: (IdObj, IdAtt(Ai), Ts1-ts2, Ic)
  
 * Sur le noeud pour la clé: IdObj || IdAtt || J(Ts1-ts2) || Ic ; J(Ts1-ts2) est une unité entière de l'intervalle de temps 

Pour retrouver l'objet, c'est les premiers bits puisque c'est ID de l'objet


Pour rechercher un point ST -> Trouver le I, puis aller sur le noeud