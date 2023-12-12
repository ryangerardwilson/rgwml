use std::fmt::Display;

#[derive(Debug, Clone, PartialEq)]
struct Vertex<T: Clone + std::fmt::Debug + Display> {
    data: Vec<T>,
}

#[derive(Debug, Clone)]
struct Edge<T: Clone + std::fmt::Debug + Display> {
    start: Vertex<T>,
    end: Vertex<T>,
}

#[derive(Debug, Clone)]
struct Path<T: Clone + std::fmt::Debug + Display> {
    vertices: Vec<Vertex<T>>,
}

#[derive(Debug, Clone)]
struct Graph<T: Clone + std::fmt::Debug + Display> {
    vertices: Vec<Vertex<T>>,
    edges: Vec<Edge<T>>,
}

impl<T: Clone + std::fmt::Debug + PartialEq + Display> Graph<T> {
    fn new() -> Self {
        Graph {
            vertices: Vec::new(),
            edges: Vec::new(),
        }
    }

    fn add_vertex(&mut self, data: Vec<T>) -> Vertex<T> {
        let vertex = Vertex { data };
        self.vertices.push(vertex.clone());
        vertex
    }

    fn add_edge(&mut self, start: Vertex<T>, end: Vertex<T>) {
        let edge = Edge { start, end };
        self.edges.push(edge);
    }

    fn edge_exists(&self, start: &Vertex<T>, end: &Vertex<T>) -> bool {
        self.edges.iter().any(|edge| {
            (edge.start == *start && edge.end == *end) || (edge.start == *end && edge.end == *start)
        })
    }

    fn get_neighbors(&self, vertex: &Vertex<T>) -> Vec<Vertex<T>> {
        self.edges
            .iter()
            .filter_map(|edge| {
                if edge.start == *vertex {
                    Some(edge.end.clone())
                } else if edge.end == *vertex {
                    Some(edge.start.clone())
                } else {
                    None
                }
            })
            .collect()
    }

    fn find_paths(
        &self,
        start: &Vertex<T>,
        end: &Vertex<T>,
        current_path: &mut Path<T>,
        all_paths: &mut Vec<Path<T>>,
    ) {
        if start == end {
            if current_path.vertices.len() >= 3 {
                all_paths.push(current_path.clone());
            }
            return;
        }

        for neighbor in self.get_neighbors(start) {
            if !current_path.vertices.contains(&neighbor) {
                current_path.vertices.push(neighbor.clone());
                self.find_paths(&neighbor, end, current_path, all_paths);
                current_path.vertices.pop();
            }
        }
    }

    fn find_all_paths(&self) -> Vec<(Vertex<T>, Vertex<T>, Vec<Path<T>>)> {
        let mut all_paths = Vec::new();
        for start in &self.vertices {
            for end in &self.vertices {
                if start != end {
                    let mut current_path = Path {
                        vertices: vec![start.clone()],
                    };
                    let mut paths = Vec::new();
                    self.find_paths(start, end, &mut current_path, &mut paths);
                    // Only add to all_paths if there are paths with at least 3 vertices
                    if !paths.is_empty() {
                        all_paths.push((start.clone(), end.clone(), paths));
                    }
                }
            }
        }
        all_paths
    }


fn traverse_pattern(&self, pattern_vertex: &Vertex<T>) -> Vec<Vec<T>> {
    // Ensure the vertex represents a pattern
    if pattern_vertex.data.len() > 1 {
        let mut sequences = vec![Vec::new()];

        // Iterate over each category in the pattern
        for category in &pattern_vertex.data {
            let mut new_sequences = Vec::new();

            // Find all vertices linked to this category
            let linked_vertices = self.find_linked_vertices(category);

            // For each sequence so far, append each linked vertex to create new sequences
            for sequence in &sequences {
                for vertex in &linked_vertices {
                    let mut new_sequence = sequence.clone();
                    new_sequence.extend(vertex.data.clone());
                    new_sequences.push(new_sequence);
                }
            }

            sequences = new_sequences;
        }

        // Return the generated sequences
        sequences
    } else {
        vec![] // or handle the error according to your needs
    }
}


    fn find_linked_vertices(&self, category: &T) -> Vec<&Vertex<T>> {
        self.edges
            .iter()
            .filter_map(|edge| {
                if edge.start.data.contains(category) {
                    Some(&edge.end)
                } else if edge.end.data.contains(category) {
                    Some(&edge.start)
                } else {
                    None
                }
            })
            .collect()
    }

    fn print_graph(&self) {
        println!("Graph:");
        println!("Vertices:");
        for vertex in &self.vertices {
            println!(" - {:?}", vertex.data);
        }

        println!("Edges:");
        for edge in &self.edges {
            println!(" - {:?} -> {:?}", edge.start.data, edge.end.data);
        }

        dbg!(&self.edges);

        // Print paths
        println!("Paths:");
        let all_paths = self.find_all_paths();
        for (start, end, paths) in all_paths {
            for path in paths {
                let path_str = path
                    .vertices
                    .iter()
                    .map(|v| format!("{:?}", v.data))
                    .collect::<Vec<String>>()
                    .join(" -> ");
                println!("Path from {:?} to {:?}: {}", start.data, end.data, path_str);
            }
        }
    }
}

fn main() {
    let mut graph = Graph::new();

    // Special Category Vertices
    let category_determiner = graph.add_vertex(vec!["Category: Determiner"]);
    let category_noun = graph.add_vertex(vec!["Category: Noun"]);
    let category_verb = graph.add_vertex(vec!["Category: Verb"]);
    let category_adjective = graph.add_vertex(vec!["Category: Adjective"]);
    let category_adverb = graph.add_vertex(vec!["Category: Adverb"]);

    let determiner1 = graph.add_vertex(vec!["a"]);
    let determiner2 = graph.add_vertex(vec!["an"]);
    let determiner3 = graph.add_vertex(vec!["the"]);
    graph.add_edge(category_determiner.clone(), determiner1.clone());
    graph.add_edge(category_determiner.clone(), determiner2.clone());
    graph.add_edge(category_determiner.clone(), determiner3.clone());

    // Adding subjects
    let noun1 = graph.add_vertex(vec!["boy"]);
    let noun2 = graph.add_vertex(vec!["girl"]);
    let noun3 = graph.add_vertex(vec!["dog"]);
    // Linking subjects to their category
    graph.add_edge(category_noun.clone(), noun1.clone());
    graph.add_edge(category_noun.clone(), noun2.clone());
    graph.add_edge(category_noun.clone(), noun3.clone());

    // Adding verbs
    let verb1 = graph.add_vertex(vec!["eats"]);
    let verb2 = graph.add_vertex(vec!["speaks"]);
    let verb3 = graph.add_vertex(vec!["barks"]);
    // Linking verbs to their category
    graph.add_edge(category_verb.clone(), verb1.clone());
    graph.add_edge(category_verb.clone(), verb2.clone());
    graph.add_edge(category_verb.clone(), verb3.clone());

    // Adding adjectives
    let adjective1 = graph.add_vertex(vec!["pretty"]);
    let adjective2 = graph.add_vertex(vec!["ugly"]);
    let adjective3 = graph.add_vertex(vec!["superb"]);
    // Linking adjectives to their category
    graph.add_edge(category_adjective.clone(), adjective1.clone());
    graph.add_edge(category_adjective.clone(), adjective2.clone());
    graph.add_edge(category_adjective.clone(), adjective3.clone());

    // Adding adverbs
    let adverb1 = graph.add_vertex(vec!["quickly"]);
    let adverb2 = graph.add_vertex(vec!["slowly"]);
    let adverb3 = graph.add_vertex(vec!["smartly"]);
    // Linking adverbs to their category
    graph.add_edge(category_adverb.clone(), adverb1.clone());
    graph.add_edge(category_adverb.clone(), adverb2.clone());
    graph.add_edge(category_adverb.clone(), adverb3.clone());

    // Define sentence pattern categories
    let pattern_nv = graph.add_vertex(vec!["Category: Noun", "Category: Verb"]);
    let pattern_nvn = graph.add_vertex(vec!["Category: Noun", "Category: Verb", "Category: Noun"]);
    let pattern_nvjn = graph.add_vertex(vec![
        "Category: Noun",
        "Category: Verb",
        "Category: Adjective",
        "Category: Noun",
    ]);
    let pattern_nevn = graph.add_vertex(vec![
        "Category: Noun",
        "Category: Adverb",
        "Category: Verb",
        "Category: Noun",
    ]);

    let pattern_dnvdn = graph.add_vertex(vec![
        "Category: Determiner",
        "Category: Noun",
        "Category: Verb",
        "Category: Determiner",
        "Category: Noun",
    ]);

    // Call traverse_pattern with the correct number of arguments
    let dnvdn_paths = graph.traverse_pattern(&pattern_dnvdn);
    
    /*
    dbg!(&all_sentences);

        for sentence in all_sentences {
        println!("{}", sentence);
    }
    */

    // println!("Graph: {:?}", graph);

    dbg!(graph);
    dbg!(dnvdn_paths);
}
