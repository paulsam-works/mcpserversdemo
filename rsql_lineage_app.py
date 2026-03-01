"""
RSQL to Lineage Converter - Gradio Web App
Converts Redshift/RSQL ETL scripts into structured lineage JSON and a graphical view.
"""

import os
import tempfile

import gradio as gr
from dotenv import load_dotenv
from pydantic import BaseModel, Field
from typing import List
from langchain_core.prompts import ChatPromptTemplate
from langchain_openai import ChatOpenAI

# Optional: use networkx + matplotlib for graph (no graphviz binary required)
try:
    import networkx as nx
    import matplotlib
    matplotlib.use("Agg")
    import matplotlib.pyplot as plt
    HAS_PLOTTING = True
except ImportError:
    HAS_PLOTTING = False

load_dotenv(override=True)


# --- Pydantic models (aligned with notebook) ---
class LineageStep(BaseModel):
    operation: str = Field(description="The SQL operation: COPY, DELETE, INSERT, etc.")
    source_name: str = Field(description="The source table or S3 path")
    target_name: str = Field(description="The destination table")
    logic: str = Field(description="Short description of the transformation logic")


class LineageFull(BaseModel):
    script_name: str = Field(description="Name of the script")
    lineage: List[LineageStep] = Field(description="List of lineage steps")


# --- Prompts (single variable to avoid ChatPromptTemplate KeyError) ---
SYSTEM_PROMPT = """
You are a Senior Data Architect specializing in Metadata Management and Collibra Data Governance.
Your task is to parse RSQL (Amazon Redshift) ETL scripts and generate a technical data lineage map
in a structured format.

### COMPLEXITY HANDLING
1. NON-SQL CONTENT: Ignore RSQL meta-commands (e.g. \\set, \\echo, \\if, \\gset) unless they define variables used in SQL.
2. DYNAMIC PARAMETERS: If the SQL has variables (e.g. :v_s3_path or ${{VAR}}), infer physical locations from context.

### OBJECTIVE
Analyze the provided SQL to identify every movement of data: external sources (S3), staging (Temp Tables), and final targets.

### EXTRACTION RULES
1. SOURCE: Look for COPY commands (S3 buckets, manifests, PARQUET).
2. STAGING: Identify CREATE TEMP TABLE / CREATE TABLE as intermediate hops.
3. TARGET: Identify final tables via INSERT INTO or MERGE.
4. OPERATIONS: DELETE + INSERT on same key = UPSERT; TRUNCATE + INSERT = FULL_RELOAD.

### CONSTRAINTS
- Do not include administrative SQL (SET, ECHO) in lineage steps.
- Chain steps so target of one step = source of the next when applicable.
- Return only the structured output; no conversational filler.
"""

USER_PROMPT_TEMPLATE = """
Analyze this SQL script and extract the lineage steps.

PARAMETER CONTEXT (if any):
{param_context}

RSQL SCRIPT TO ANALYZE:
{sql_code}
"""


def build_chain():
    """Build the LangChain pipeline. Uses single input variable to avoid template errors."""
    llm = ChatOpenAI(model="gpt-4o", temperature=0)
    structured_llm = llm.with_structured_output(LineageFull)
    # Use one placeholder: input_text (combined param_context + sql_code)
    prompt = ChatPromptTemplate.from_messages([
        ("system", SYSTEM_PROMPT),
        ("user", "{input_text}"),
    ])
    return prompt | structured_llm


def draw_lineage_graph(lineage_data: LineageFull) -> str | None:
    """Render lineage as a graph image. Returns path to temporary PNG or None if not available."""
    if not HAS_PLOTTING or not lineage_data.lineage:
        return None

    G = nx.DiGraph()
    for step in lineage_data.lineage:
        G.add_edge(
            step.source_name,
            step.target_name,
            operation=step.operation,
        )

    if G.number_of_edges() == 0:
        return None

    fig, ax = plt.subplots(figsize=(12, 6))
    pos = nx.spring_layout(G, k=2.5, seed=42)
    node_colors = []
    for n in G.nodes():
        if "s3://" in n.lower() or "s3:" in n.lower():
            node_colors.append("#2ecc71")  # green for S3
        elif "temp" in n.lower() or "stage" in n.lower() or "stg" in n.lower():
            node_colors.append("#3498db")  # blue for staging
        else:
            node_colors.append("#9b59b6")  # purple for target

    edge_colors = {
        "COPY": "#2ecc71",
        "INSERT": "#9b59b6",
        "DELETE": "#e74c3c",
        "UPSERT": "#3498db",
        "MAINTENANCE": "#e67e22",
    }
    edge_color_list = [
        edge_colors.get(e[2]["operation"].upper(), "#7f8c8d")
        for e in G.edges(data=True)
    ]

    nx.draw_networkx_nodes(
        G, pos, node_color=node_colors, node_size=2000,
        alpha=0.9, ax=ax, node_shape="s"
    )
    nx.draw_networkx_labels(
        G, pos, font_size=8, ax=ax,
        labels={n: n if len(n) <= 35 else n[:32] + "..." for n in G.nodes()}
    )
    nx.draw_networkx_edges(
        G, pos, edge_color=edge_color_list, width=2,
        arrows=True, arrowsize=20, ax=ax,
        connectionstyle="arc3,rad=0.1"
    )
    edge_labels = {(u, v): d["operation"] for u, v, d in G.edges(data=True)}
    nx.draw_networkx_edge_labels(G, pos, edge_labels, font_size=8, ax=ax)

    ax.set_title("Data Lineage", fontsize=14, fontweight="bold")
    ax.axis("off")
    plt.tight_layout()

    out_path = tempfile.NamedTemporaryFile(suffix=".png", delete=False).name
    plt.savefig(out_path, dpi=120, bbox_inches="tight", facecolor="white")
    plt.close()
    return out_path


def run_rsql_to_lineage(
    sql_code: str,
    param_context: str = "",
    progress=gr.Progress(),
) -> tuple[str, str | None, str]:
    """
    Run the lineage extraction. Returns (json_str, image_path, error_message).
    """
    if not sql_code or not sql_code.strip():
        return "{}", None, "Please enter RSQL in the left panel."

    api_key = os.getenv("OPENAI_API_KEY")
    if not api_key:
        return "{}", None, "OPENAI_API_KEY is not set. Add it to .env in the app directory."

    progress(0.2, desc="Building chain…")
    try:
        chain = build_chain()
    except Exception as e:
        return "{}", None, f"Chain build failed: {e}"

    progress(0.4, desc="Extracting lineage…")
    user_input = USER_PROMPT_TEMPLATE.format(
        param_context=param_context.strip() or "(None provided)",
        sql_code=sql_code.strip(),
    )
    try:
        result = chain.invoke({"input_text": user_input})
    except Exception as e:
        return "{}", None, f"LLM invocation failed: {e}"

    progress(0.8, desc="Rendering graph…")
    json_str = result.model_dump_json(indent=2)
    image_path = draw_lineage_graph(result)
    return json_str, image_path, ""


# --- Gradio UI ---
custom_css = """
/* Professional dark sidebar + light content */
.gradio-container { font-family: 'Segoe UI', system-ui, sans-serif; }
.primary-btn { font-weight: 600; }
/* Panel separation */
.panel-left { border-right: 1px solid #e0e0e0; padding-right: 1rem; }
.panel-right { padding-left: 1rem; }
/* Headers */
.panel-title { font-size: 1.1rem; font-weight: 600; color: #1a1a2e; margin-bottom: 0.5rem; }
/* JSON block */
.json-block { font-family: 'Consolas', 'Monaco', monospace; font-size: 12px; }
"""

with gr.Blocks(
    title="RSQL Lineage Converter",
    theme=gr.themes.Soft(
        primary_hue="indigo",
        secondary_hue="slate",
    ),
    css=custom_css,
) as app:

    gr.Markdown(
        """
        # RSQL to Lineage Converter
        **Parse Redshift/RSQL ETL scripts → structured lineage JSON + graph**
        """
    )

    with gr.Row():
        # --- Left panel ---
        with gr.Column(scale=1, elem_classes=["panel-left"]):
            gr.Markdown("### Input", elem_classes=["panel-title"])
            sql_input = gr.Textbox(
                label="RSQL script",
                placeholder="Paste your Redshift/RSQL ETL script here…\n\nExample:\nCOPY stage_temp FROM 's3://bucket/path' IAM_ROLE 'arn:...' FORMAT AS PARQUET;\nINSERT INTO target_table SELECT * FROM stage_temp;",
                lines=18,
                max_lines=30,
                show_copy_button=True,
            )
            param_input = gr.Textbox(
                label="Parameter context (optional)",
                placeholder="Variable values, e.g. SRC_BUCKET=s3://my-bucket/ …",
                lines=4,
                show_copy_button=True,
            )
            run_btn = gr.Button("Execute", variant="primary", elem_classes=["primary-btn"])

        # --- Right panel ---
        with gr.Column(scale=1, elem_classes=["panel-right"]):
            gr.Markdown("### Extracted lineage", elem_classes=["panel-title"])
            error_out = gr.Textbox(
                label="Status",
                interactive=False,
                visible=True,
            )
            with gr.Tabs():
                with gr.Tab("JSON"):
                    json_out = gr.Code(
                        label="Lineage (JSON)",
                        language="json",
                        lines=14,
                        elem_classes=["json-block"],
                    )
                with gr.Tab("Graph"):
                    graph_out = gr.Image(
                        label="Lineage graph",
                        type="filepath",
                    )

    def run_and_show(sql_code, param_context, progress=gr.Progress()):
        json_str, image_path, err = run_rsql_to_lineage(sql_code, param_context, progress)
        if err:
            return json_str or "{}", image_path, err
        return json_str, image_path, "Done."

    run_btn.click(
        fn=run_and_show,
        inputs=[sql_input, param_input],
        outputs=[json_out, graph_out, error_out],
    )

    # Example in the description or a tab
    gr.Markdown(
        """
        ---
        **Usage:** Paste RSQL in the left panel, optionally add parameter context, then click **Execute**.  
        Right panel shows the lineage JSON and a graph. Ensure `OPENAI_API_KEY` is set in `.env`.
        """
    )


if __name__ == "__main__":
    app.launch()
