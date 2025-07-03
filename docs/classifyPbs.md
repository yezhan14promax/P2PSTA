# Voici les problèmes qui doivent guider le design d'une solution

## Doit on indexer des points ou des segments

beaucoup de points à indexer. Avec un modèle basique, si chaque point contient des attribus (i.e. un tuple), le volume des données devien prohibitive
As TS may be long (e.g., 30 GBs), a peer handling an entire TS might be overloaded, in particular for popular TSs.
To avoid this kind of bottleneck, we introduce a method to distribute long TSs into slices on a ring-like addressing space. At loading time, the system distributes TS over the network based on a random hash function. Long TS are split into a sequence of segments. Segments are assigned to peers. Conversely, peers maintain in cache TS segments either imported or calculated. Peers publish the segments they have in cache to other peers by inserting a record in a network DHT (note that we assume this network manages connect, disconnect and replication issues). Every segment has the same length (e.g., 1024 entries for stocks).

Donc, il faut indexer suivant les rectangles d'une grille
Etat de l'art:

| Ref | Type index |  |
|---------|:-----:|---------:|
|  |  |  |

Ref, P2P, type de segmentation, remarque
[Gar10] Gestion efficace de séries temporelles en P2P: Application à l'analyse technique et l'étude des objets mobiles
