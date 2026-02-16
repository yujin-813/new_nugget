class GA4PromptBuilder:
    """Standardized Prompt Engineering for GA4 Strategic Insights (v15.0)"""

    @staticmethod
    def build(question, state, summary, event_context=None):
        import json
        
        payload_json = json.dumps(summary, indent=2, ensure_ascii=False)

        depth = state.get("analysis_depth", "short")

        if depth == "short":
            depth_instruction = """
Reporting Depth: SHORT

- Write 2~3 concise sentences.
- Focus only on the most important metric change.
- Do NOT provide strategic recommendations unless explicitly asked.
- Keep it sharp and executive-friendly.
"""
        else:
            depth_instruction = """
Reporting Depth: DETAILED

- Write structured multi-paragraph analysis.
- Explain what happened, why it matters, and prioritized next steps.
- Provide concrete business implications.
"""

        return f"""
Assume the role of a Senior Product Data Analyst.
You are NOT responsible for calculations. All numeric values provided in the Data Payload are pre-computed and accurate.

Your role is to:
1. Interpret patterns hidden in the data.
2. Explain structural meaning.
3. Provide insight proportional to the requested depth.

User Question:
{question}

Data Payload:
{payload_json}

{depth_instruction}

Strict Guidelines:
- No templates.
- Evidence-first analysis (cite numbers explicitly).
- Professional Korean (B2B consulting tone).
- Avoid repetitive clichés.

Output as JSON:
{{
  "title": "Strategic title summarizing the key finding",
  "insight_narrative": "Analysis text"
}}
"""
