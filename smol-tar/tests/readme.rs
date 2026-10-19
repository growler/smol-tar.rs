//! Make sure that each README example is a copy of an example program.
//!
//! In README.md, the line `<!-- example: examples/NAME.rs -->` comes
//! immediately before a `rust` code block. The code block must be equal to
//! the file. `cargo test` compiles the example programs.

use std::path::Path;

#[test]
fn readme_examples_match_example_programs() {
    let root = Path::new(env!("CARGO_MANIFEST_DIR"));
    let readme = std::fs::read_to_string(root.join("README.md")).unwrap();
    let mut lines = readme.lines();
    let mut count = 0;
    while let Some(line) = lines.next() {
        let Some(path) = line
            .strip_prefix("<!-- example: ")
            .and_then(|rest| rest.strip_suffix(" -->"))
        else {
            continue;
        };
        assert_eq!(lines.next(), Some("```rust"), "no code block after {path}");
        let block: Vec<&str> = lines.by_ref().take_while(|l| *l != "```").collect();
        let source = std::fs::read_to_string(root.join(path)).unwrap();
        assert_eq!(
            block.join("\n"),
            source.trim_end(),
            "README.md code block differs from {path}"
        );
        count += 1;
    }
    assert!(count > 0, "README.md has no example markers");
}
