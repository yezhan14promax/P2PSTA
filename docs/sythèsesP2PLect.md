

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

  * a un identifiant clé [MANN][SWORD]
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

## Contexte

En distribué, comment répondre à des requêtes de types:
 
 * Q1: Les personnes qui ont visitée la Tour Eiffel ? S
 * Q2: Les personnes qui ont visitée la Tour Eiffel le 3 juin ? ST
 * Q3: Les personnes qui ont visitée la Tour Eiffel puis sont allées visiter Notre Dame ? SZ 
 * Q4: Les personnes qui ont visitée la Tour Eiffel le 3 juin puis sont allées visiter Notre Dame ? STZ
 * Q5: Les personnes qui ont visitée la Tour Eiffel et ayant pris le Bus 57 ? SA
 * Q6: Les personnes qui ont visitée la Tour Eiffel le 3 juin à 2h et ayant pris le Bus 57 ? STA
 * Q7: Les personnes qui ont visitée la Tour Eiffel et ayant pris le Bus 57, puis sont allées visiter Notre Dame en ayant pris la ligne 1 du métro ? STA


## Problème:

Pb1: Un objet trajectoire contient beaucoup de points vs un objet spatio temporel.

  * Conséquence: indexer tous ces points devient prohibitives.

  * Les modèles vus sont ST pour des points et ne sont pas adaptés pour des trajectoires.

Opposer les modèles points vs modèles séquences.
  * Un modèle point considérere chaque enregistremenet/donnée comme une coordonnée dans l'espace temps.
    * Certains modèles permettent de faire du range query sur ces modèles points. [Mann][MutiVor]

Pb2: Lorsqu'on indexe une trajectoire qui évolue au cours du temps, dans le cas de multi-attributs, les attribus évoluent également dans le temps. 


If we consider multi-attributes spatio-temporal objet (i.e. trajectory)

## Propositions:
Contribution: **Index non dense** pour les trajectoires

 * Un modèle en versionning et non ponctuel
 * Un index *non denses* basé sur un découpage de la clé en unité discrete
 * Basé sur une extension de Dynamo pour le modèle multiattribut
 * Une espace d'addressage de Chord en range
 * Une vision inspirée des bases de données avec des index primaires et des index secondaires.
 * Des super-peers qui rassemblent les morceaux de trajectoires

## Principe du modèle:

Une tesselation de l'espace et du temps en cellules { C }

Z = trajectoire ST

R = Un objet STA A = {a1, ..., an} (A1,...An)

{ I } = Z INTO C ; I est l'identifiant des cellules qui forme une clé

{ Ic } = Z INTERSECT C ; Ic ce sont des morceaux de trajectoires

Nodes(I) <- R

### Chaque objet est indexé

B(v) est une decomposition en intervalles. Exemple possible B(v) = v div 1000
|| concatenation de bits


NOTE: Key est une concaténation de bits 

#### Index primaire
Nodes(Key(I || T)) <- R  
  * Ce qu'on stocke c'est tous les points dans l'espace et le temps.
  * C'est l'index primaire

#### Index secondaire
Nodes(Key(T || B(a_i))) <- R(a_i) U {Pt vers Key(I || T)}
  * Si besoin un index secondaire.

* Pourquoi dans notre cas, c'est des index non denses ? A cause de B()

* Si un pair ST peut prendre sur plusieurs intervalles de temps, alors fusion des objets


### Résolution des requêtes

A? Avec ou sans des attributs non spatial et temporel
T represente un attribut temporel

Q = p_1 ^ ... ^ p_n
type(p_i) est soit S)patial, T)emporel, A)ttributaire 

* cas ST + A?: Tous les Key(I || T) sont des noeuds à rechercher. Résolution de A? au niveau des noeuds
* cas S + A?: Tous les Key(I || *). Tous les noeuds T sont contactés et Résolution de A?
* cas T + A?: 
  - Si un a_i est indexé, alors route vers le Nodes(Key(T || B(a_i))), puis Nodes(Key(I || T))
     Cas discutable, car pas sûr de qui est le plus séléctif !
  - sinon Tous les Key(* || T). Tous les noeuds T sont contactés et résolution de A?
* cas T + A?: Tous les Key(* || T). Tous les noeuds T sont contactés et résolution de A?
* cas a_i est indexé: Nodes(Key(* || a_i)) -> Nodes(Key(I || T))
* Sinon, pas authorisé ou un scan de tous les noeuds !
  
### Résolution des ranges queries

* p_i = v, 
Un range Queries revient à determiner l'ensemble des tesselations C ou B qui peuvent répondre à la requête.


# Poubelle
Indexer chaque colonne independament. (IdObj, IdAtt(Ai), Ts, Val) -> Dynamo
   
 * Sur le noeud pour la clé: IdObj || IdAtt || J(Ts)  J est une décomposition en intervalle du temps

Mais pour le spatial: (IdObj, IdAtt(Ai), Ts1-ts2, I)
  
 * Sur le noeud pour la clé: IdObj || IdAtt || J(Ts1-ts2) || Ic ; J(Ts1-ts2) est une unité entière de l'intervalle de temps 

Pour retrouver l'objet, c'est les premiers bits puisque c'est ID de l'objet


Pour rechercher un point ST -> Trouver le I, puis aller sur le noeud