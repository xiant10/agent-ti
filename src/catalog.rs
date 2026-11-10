//! BM25 search index, tokenizer, and tool pattern matching over the tool catalog.
use std::collections::{HashMap, HashSet};

const BM25_K1: f64 = 1.2;
const BM25_B: f64 = 0.75;
const NAME_WEIGHT: f64 = 3.0;

/// Returns true if a character belongs to Han, Hiragana, Katakana, or Hangul scripts.
pub fn is_cjk(c: char) -> bool {
    matches!(
        c,
        '\u{4e00}'..='\u{9fff}'
            | '\u{3400}'..='\u{4dbf}'
            | '\u{20000}'..='\u{2a6df}'
            | '\u{f900}'..='\u{faff}'
            | '\u{3040}'..='\u{309f}'
            | '\u{30a0}'..='\u{30ff}'
            | '\u{ac00}'..='\u{d7af}'
            | '\u{1100}'..='\u{11ff}'
            | '\u{3130}'..='\u{318f}'
            | '\u{3100}'..='\u{312f}'
    )
}

/// Tokenizer for search: lowercase ASCII words (camelCase and snake_case split),
/// and for CJK text, every character and every adjacent pair.
pub fn tokenize(text: &str) -> Vec<String> {
    let mut tokens = Vec::new();
    // Space out camelCase boundaries: lower/digit followed by upper
    let mut spaced = String::with_capacity(text.len() + 8);
    let mut prev: Option<char> = None;
    for c in text.chars() {
        if let Some(p) = prev
            && (p.is_ascii_lowercase() || p.is_ascii_digit())
            && c.is_ascii_uppercase()
        {
            spaced.push(' ');
        }
        spaced.push(c);
        prev = Some(c);
    }

    // Split on non-letter and non-number characters
    let runs = spaced
        .to_lowercase()
        .split(|c: char| !c.is_alphanumeric())
        .map(str::to_owned)
        .collect::<Vec<_>>();

    for run in runs {
        if run.is_empty() {
            continue;
        }
        let mut latin = String::new();
        let mut cjk = Vec::new();

        let flush_latin = |latin: &mut String, tokens: &mut Vec<String>| {
            if latin.len() > 1 || latin.chars().any(|c| c.is_ascii_digit()) {
                tokens.push(latin.clone());
            }
            latin.clear();
        };

        let flush_cjk = |cjk: &mut Vec<char>, tokens: &mut Vec<String>| {
            for i in 0..cjk.len() {
                tokens.push(cjk[i].to_string());
                if i + 1 < cjk.len() {
                    tokens.push(format!("{}{}", cjk[i], cjk[i + 1]));
                }
            }
            cjk.clear();
        };

        for ch in run.chars() {
            if is_cjk(ch) {
                flush_latin(&mut latin, &mut tokens);
                cjk.push(ch);
            } else {
                flush_cjk(&mut cjk, &mut tokens);
                latin.push(ch);
            }
        }
        flush_latin(&mut latin, &mut tokens);
        flush_cjk(&mut cjk, &mut tokens);
    }

    tokens
}

#[derive(Clone, Debug)]
pub struct CatalogTool {
    pub name: String,
    pub server: String,
    pub mcp_name: String,
    pub description: String,
    /// The tool's owner as the tool gateway names it (`ToolEntry::owner`).
    pub owner: Option<String>,
}

#[derive(Clone, Debug)]
pub struct SearchHit {
    pub tool: CatalogTool,
    pub score: f64,
}

struct IndexedDoc {
    tool: CatalogTool,
    terms: HashMap<String, f64>,
    length: f64,
}

/// BM25 index over tool names and descriptions.
pub struct SearchIndex {
    docs: Vec<IndexedDoc>,
    document_frequency: HashMap<String, usize>,
    average_length: f64,
}

impl SearchIndex {
    pub fn new(tools: &[CatalogTool]) -> Self {
        let mut docs = Vec::new();
        let mut df: HashMap<String, usize> = HashMap::new();
        let mut total_length = 0.0;

        for tool in tools {
            let mut terms: HashMap<String, f64> = HashMap::new();
            let mut add = |token: String, weight: f64| {
                *terms.entry(token).or_insert(0.0) += weight;
            };
            for token in tokenize(&format!("{} {}", tool.mcp_name, tool.server)) {
                add(token, NAME_WEIGHT);
            }
            for token in tokenize(&tool.description) {
                add(token, 1.0);
            }
            let length: f64 = terms.values().sum();
            total_length += length;
            for term in terms.keys() {
                *df.entry(term.clone()).or_insert(0) += 1;
            }
            docs.push(IndexedDoc {
                tool: tool.clone(),
                terms,
                length,
            });
        }

        let average_length = if docs.is_empty() {
            1.0
        } else {
            total_length / (docs.len() as f64)
        };

        Self {
            docs,
            document_frequency: df,
            average_length,
        }
    }

    pub fn search(&self, query: &str, limit: usize) -> Vec<SearchHit> {
        let query_terms: Vec<String> = {
            let mut seen = HashSet::new();
            tokenize(query)
                .into_iter()
                .filter(|t| seen.insert(t.clone()))
                .collect()
        };
        if query_terms.is_empty() {
            return Vec::new();
        }
        let n = self.docs.len() as f64;
        let mut hits = Vec::new();

        for doc in &self.docs {
            let mut score = 0.0;
            for term in &query_terms {
                if let Some(&frequency) = doc.terms.get(term) {
                    let df = *self.document_frequency.get(term).unwrap_or(&0) as f64;
                    let idf = (1.0 + (n - df + 0.5) / (df + 0.5)).ln();
                    let norm = frequency
                        + BM25_K1 * (1.0 - BM25_B + (BM25_B * doc.length) / self.average_length);
                    score += (idf * frequency * (BM25_K1 + 1.0)) / norm;
                }
            }
            if score > 0.0 {
                hits.push(SearchHit {
                    tool: doc.tool.clone(),
                    score,
                });
            }
        }

        hits.sort_by(|a, b| {
            b.score
                .partial_cmp(&a.score)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| a.tool.name.cmp(&b.tool.name))
        });
        hits.truncate(limit);
        hits
    }
}

/// Matches a glob pattern containing `*` and `?`.
pub fn glob_matches(pattern: &str, text: &str) -> bool {
    let p_bytes = pattern.as_bytes();
    let t_bytes = text.as_bytes();
    let mut p_idx = 0;
    let mut t_idx = 0;
    let mut p_star: Option<usize> = None;
    let mut t_match = 0;

    while t_idx < t_bytes.len() {
        if p_idx < p_bytes.len() && (p_bytes[p_idx] == b'?' || p_bytes[p_idx] == t_bytes[t_idx]) {
            p_idx += 1;
            t_idx += 1;
        } else if p_idx < p_bytes.len() && p_bytes[p_idx] == b'*' {
            p_star = Some(p_idx);
            p_idx += 1;
            t_match = t_idx;
        } else if let Some(star) = p_star {
            p_idx = star + 1;
            t_match += 1;
            t_idx = t_match;
        } else {
            return false;
        }
    }

    while p_idx < p_bytes.len() && p_bytes[p_idx] == b'*' {
        p_idx += 1;
    }

    p_idx == p_bytes.len()
}

/// Matches tool patterns `[server:]glob` against a tool.
pub fn pattern_matches(pattern: &str, server: &str, mcp_name: &str, offered_name: &str) -> bool {
    let (target_server, glob) = match pattern.split_once(':') {
        Some((srv, g)) => (Some(srv), g),
        None => (None, pattern),
    };
    if let Some(srv) = target_server
        && srv != server
        && srv != "*"
    {
        return false;
    }
    glob_matches(glob, mcp_name) || glob_matches(glob, offered_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_tokenize() {
        assert_eq!(
            tokenize("listOpenInvoices get_account"),
            vec!["list", "open", "invoices", "get", "account"]
        );
        assert_eq!(
            tokenize("查询客户"),
            vec!["查", "查询", "询", "询客", "客", "客户", "户"]
        );
    }

    #[test]
    fn test_bm25_search() {
        let tools = vec![
            CatalogTool {
                name: "crm_list_accounts".into(),
                server: "crm".into(),
                mcp_name: "list_accounts".into(),
                description: "List customer accounts.".into(),
                owner: None,
            },
            CatalogTool {
                name: "crm_get_invoice".into(),
                server: "crm".into(),
                mcp_name: "get_invoice".into(),
                description: "Fetch an invoice with its due date.".into(),
                owner: None,
            },
            CatalogTool {
                name: "crm_search_profile".into(),
                server: "crm".into(),
                mcp_name: "search_profile".into(),
                description: "查询客户画像和档案".into(),
                owner: None,
            },
        ];

        let index = SearchIndex::new(&tools);
        let hits1 = index.search("when is the invoice due", 1);
        assert_eq!(hits1.len(), 1);
        assert_eq!(hits1[0].tool.mcp_name, "get_invoice");

        let hits2 = index.search("帮我查一下客户档案", 1);
        assert_eq!(hits2.len(), 1);
        assert_eq!(hits2[0].tool.mcp_name, "search_profile");
    }

    #[test]
    fn test_pattern_matching() {
        assert!(pattern_matches(
            "crm:list_*",
            "crm",
            "list_accounts",
            "crm_list_accounts"
        ));
        assert!(pattern_matches(
            "crm:list_*",
            "crm",
            "list_deals",
            "crm_list_deals"
        ));
        assert!(!pattern_matches(
            "crm:list_*",
            "crm",
            "get_deal",
            "crm_get_deal"
        ));
        assert!(!pattern_matches(
            "crm:list_*",
            "cal",
            "list_events",
            "cal_list_events"
        ));

        assert!(pattern_matches(
            "list_events",
            "cal",
            "list_events",
            "cal_list_events"
        ));
        assert!(pattern_matches("crm:*", "crm", "anything", "crm_anything"));
    }
}
