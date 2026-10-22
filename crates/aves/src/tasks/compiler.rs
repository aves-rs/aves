use std::path::Path;

use oxc::allocator::Allocator;
use oxc::codegen::Codegen;
use oxc::diagnostics::Diagnostics;
use oxc::parser::Parser;
use oxc::semantic::SemanticBuilder;
use oxc::span::SourceType;
use oxc::transformer::{TransformOptions, Transformer};

use super::types::TasksError;

fn format_diagnostics(diagnostics: Diagnostics, source: &str) -> String {
    diagnostics
        .into_iter()
        .fold(String::new(), |mut message, diagnostic| {
            if !message.is_empty() {
                message.push('\n');
            }
            message.push_str(&format!("{:?}", diagnostic.with_source_code(source.to_owned())));
            message
        })
}

/// Compiles user-authored TypeScript into JavaScript understood by QuickJS.
#[derive(Debug, Clone, Copy, Default)]
pub(super) struct TypeScriptCompiler;

impl TypeScriptCompiler {
    /// Transpiles one source unit without reading or writing host files.
    pub(super) fn transpile(&self, source: &str, filename: &str) -> Result<String, TasksError> {
        let allocator = Allocator::default();
        let path = Path::new(filename);
        let source_type = SourceType::ts().with_module(true);
        let parsed = Parser::new(&allocator, source, source_type).parse();
        if !parsed.diagnostics.is_empty() {
            return Err(TasksError::Compilation {
                message: format_diagnostics(parsed.diagnostics, source),
            });
        }
        let mut program = parsed.program;
        let semantic = SemanticBuilder::new()
            .with_excess_capacity(2.0)
            .build(&program);
        if !semantic.diagnostics.is_empty() {
            return Err(TasksError::Compilation {
                message: format_diagnostics(semantic.diagnostics, source),
            });
        }
        let transformed = Transformer::new(&allocator, path, &TransformOptions::default())
            .build_with_scoping(semantic.semantic.into_scoping(), &mut program);
        if !transformed.diagnostics.is_empty() {
            return Err(TasksError::Compilation {
                message: format_diagnostics(transformed.diagnostics, source),
            });
        }
        Ok(Codegen::new().build(&program).code)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn transpiles_typescript_and_rejects_syntax_errors() {
        let compiler = TypeScriptCompiler;
        let output = compiler
            .transpile("const answer: number = 42;", "script.ts")
            .unwrap();
        assert!(output.contains("answer"));
        assert!(matches!(
            compiler.transpile("const =", "script.ts"),
            Err(TasksError::Compilation { .. })
        ));
    }

    #[test]
    fn transpiles_static_imports_as_module_source() {
        let compiler = TypeScriptCompiler;

        let output = compiler
            .transpile(
                "import { value } from './companion.js'; export const answer = value;",
                "script.ts",
            )
            .unwrap();

        assert!(output.contains("import"));
        assert!(output.contains("./companion.js"));
    }
}
