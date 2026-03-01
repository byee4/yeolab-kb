"""
Chat service layer for AI integration.
Supports Claude (Anthropic) and ChatGPT (OpenAI) with shared tool use.
"""
import json
import logging
from django.db.models import Count

from .models import (
    Publication, Author, DatasetAccession, DatasetFile,
    Grant, SraExperiment, SraRun,
)
from .ai_tools import TOOL_DEFINITIONS, execute_tool

logger = logging.getLogger(__name__)

# Maximum rounds of tool use before forcing a final answer
MAX_TOOL_ROUNDS = 8
DEFAULT_CLAUDE_MODEL = "claude-sonnet-4-20250514"
DEFAULT_OPENAI_MODEL = "gpt-4.1"


def get_system_prompt():
    """Build context-aware system prompt with live database counts."""
    try:
        pub_count = Publication.objects.count()
        author_count = Author.objects.count()
        dataset_count = DatasetAccession.objects.count()
        file_count = DatasetFile.objects.count()
        grant_count = Grant.objects.count()
        sra_exp_count = SraExperiment.objects.count()
        sra_run_count = SraRun.objects.count()

        years = Publication.objects.exclude(pub_year__isnull=True)
        min_year = years.order_by("pub_year").values_list("pub_year", flat=True).first() or "?"
        max_year = years.order_by("-pub_year").values_list("pub_year", flat=True).first() or "?"

        top_journals = list(
            Publication.objects.values("journal_name")
            .annotate(count=Count("pmid"))
            .order_by("-count")[:5]
        )
        journals_str = ", ".join(
            f'{j["journal_name"]} ({j["count"]})' for j in top_journals
        )
    except Exception:
        pub_count = author_count = dataset_count = file_count = "?"
        grant_count = sra_exp_count = sra_run_count = "?"
        min_year = max_year = "?"
        journals_str = "(unavailable)"

    return f"""You are an AI research assistant for the Yeo Lab Publications Database at UC San Diego.
You help researchers explore and understand the lab's body of work.

## Database Overview
- {pub_count} publications ({min_year}–{max_year})
- {author_count} unique authors
- {dataset_count} GEO/SRA dataset accessions
- {file_count} data files
- {grant_count} grants
- {sra_exp_count} SRA experiments, {sra_run_count} SRA runs
- Top journals: {journals_str}

The lab is led by Gene Yeo and focuses on RNA biology, RNA-binding proteins, \
post-transcriptional regulation, neurodegeneration, CRISPR, and methods like \
eCLIP, ENCODE, and single-cell genomics.

## How to Help Users
- Use the tools to search and retrieve data before answering.
- Always cite specific PMIDs when discussing papers.
- Mention GEO/SRA accessions when discussing datasets.
- For broad questions ("What does the lab work on?"), use get_database_stats first.
- For specific questions ("papers on TDP-43"), use search_publications.
- For author questions, use search_authors then get_author.
- For dataset questions, use search_datasets then get_dataset.
- Be concise but thorough. If the user asks for a summary, summarize the key findings.
- If a search returns no results, say so honestly rather than guessing.
- Format responses in markdown for readability.
"""


OPENAI_TOOL_DEFINITIONS = [
    {
        "type": "function",
        "function": {
            "name": tool["name"],
            "description": tool["description"],
            "parameters": tool["input_schema"],
        },
    }
    for tool in TOOL_DEFINITIONS
]


def stream_chat(api_key, user_message, conversation_history=None, provider="claude", model=None):
    """
    Stream a chat response with tool use.

    Yields SSE-formatted strings: "data: {json}\n\n"

    Args:
        api_key: User API key
        user_message: The new user message
        conversation_history: List of prior messages [{role, content}, ...]
        provider: "claude" or "openai"
        model: provider-specific model name (optional)
    """
    provider = (provider or "claude").lower()
    if provider == "openai":
        yield from _stream_openai(api_key, user_message, conversation_history, model or DEFAULT_OPENAI_MODEL)
        return

    if provider != "claude":
        yield _sse({"type": "error", "message": f"Unsupported provider: {provider}"})
        return

    yield from _stream_claude(api_key, user_message, conversation_history, model or DEFAULT_CLAUDE_MODEL)


def _stream_claude(api_key, user_message, conversation_history, model):
    """Claude streaming implementation with tool use."""
    try:
        import anthropic
    except ImportError:
        yield _sse({"type": "error", "message": "anthropic package not installed. Run: pip install anthropic"})
        return

    # Build messages list
    messages = list(conversation_history or [])
    messages.append({"role": "user", "content": user_message})

    # Create client with user's key
    system_prompt = get_system_prompt()

    try:
        client = anthropic.Anthropic(api_key=api_key)
    except Exception as e:
        yield _sse({"type": "error", "message": f"Failed to initialize API client: {str(e)}"})
        return

    tool_rounds = 0

    try:
        while tool_rounds <= MAX_TOOL_ROUNDS:
            # Stream the response
            collected_text = ""
            tool_uses = []

            with client.messages.stream(
                model=model,
                max_tokens=4096,
                system=system_prompt,
                messages=messages,
                tools=TOOL_DEFINITIONS,
            ) as stream:
                for event in stream:
                    if event.type == "content_block_start":
                        if event.content_block.type == "text":
                            pass  # text will come in deltas
                        elif event.content_block.type == "tool_use":
                            tool_uses.append({
                                "id": event.content_block.id,
                                "name": event.content_block.name,
                                "input_json": "",
                            })

                    elif event.type == "content_block_delta":
                        if event.delta.type == "text_delta":
                            collected_text += event.delta.text
                            yield _sse({"type": "text", "content": event.delta.text})
                        elif event.delta.type == "input_json_delta":
                            if tool_uses:
                                tool_uses[-1]["input_json"] += event.delta.partial_json

                # Get the final message for stop_reason
                final_message = stream.get_final_message()

            # If no tool calls, we're done
            if final_message.stop_reason != "tool_use" or not tool_uses:
                break

            tool_rounds += 1

            # Build the assistant message content blocks for the conversation
            assistant_content = []
            if collected_text:
                assistant_content.append({"type": "text", "text": collected_text})
            for tu in tool_uses:
                try:
                    tool_input = json.loads(tu["input_json"]) if tu["input_json"] else {}
                except json.JSONDecodeError:
                    tool_input = {}
                assistant_content.append({
                    "type": "tool_use",
                    "id": tu["id"],
                    "name": tu["name"],
                    "input": tool_input,
                })

            messages.append({"role": "assistant", "content": assistant_content})

            # Execute tools and build tool results
            tool_results = []
            for tu in tool_uses:
                try:
                    tool_input = json.loads(tu["input_json"]) if tu["input_json"] else {}
                except json.JSONDecodeError:
                    tool_input = {}

                yield _sse({
                    "type": "tool_use",
                    "name": tu["name"],
                    "input": tool_input,
                })

                result = execute_tool(tu["name"], tool_input)

                yield _sse({
                    "type": "tool_result",
                    "name": tu["name"],
                    "result": _truncate_result(result),
                })

                tool_results.append({
                    "type": "tool_result",
                    "tool_use_id": tu["id"],
                    "content": json.dumps(result, default=str),
                })

            messages.append({"role": "user", "content": tool_results})
            collected_text = ""

        yield _sse({"type": "done"})

    except ImportError as e:
        yield _sse({"type": "error", "message": f"Missing dependency: {str(e)}"})
    except anthropic.AuthenticationError:
        yield _sse({"type": "error", "message": "Invalid API key. Please check your Anthropic API key and try again."})
    except anthropic.RateLimitError:
        yield _sse({"type": "error", "message": "Rate limit exceeded. Please wait a moment and try again."})
    except anthropic.APIConnectionError:
        yield _sse({"type": "error", "message": "Could not connect to the Anthropic API. Please check your network connection."})
    except Exception as e:
        logger.exception("Chat error")
        yield _sse({"type": "error", "message": f"An error occurred: {str(e)}"})


def _stream_openai(api_key, user_message, conversation_history, model):
    """OpenAI Chat Completions streaming implementation with tool use."""
    try:
        from openai import OpenAI
    except ImportError:
        yield _sse({"type": "error", "message": "openai package not installed. Run: pip install openai"})
        return

    messages = list(conversation_history or [])
    messages.append({"role": "user", "content": user_message})
    system_prompt = get_system_prompt()

    try:
        client = OpenAI(api_key=api_key)
    except Exception as e:
        yield _sse({"type": "error", "message": f"Failed to initialize API client: {str(e)}"})
        return

    tool_rounds = 0

    try:
        while tool_rounds <= MAX_TOOL_ROUNDS:
            collected_text = ""
            tool_calls = {}
            finish_reason = None

            stream = client.chat.completions.create(
                model=model,
                messages=[{"role": "system", "content": system_prompt}, *messages],
                tools=OPENAI_TOOL_DEFINITIONS,
                tool_choice="auto",
                stream=True,
            )

            for chunk in stream:
                if not chunk.choices:
                    continue
                choice = chunk.choices[0]
                delta = choice.delta
                if choice.finish_reason:
                    finish_reason = choice.finish_reason

                if delta.content:
                    collected_text += delta.content
                    yield _sse({"type": "text", "content": delta.content})

                if delta.tool_calls:
                    for tc in delta.tool_calls:
                        idx = tc.index
                        if idx not in tool_calls:
                            tool_calls[idx] = {
                                "id": "",
                                "name": "",
                                "arguments_json": "",
                            }
                        if tc.id:
                            tool_calls[idx]["id"] = tc.id
                        if tc.function:
                            if tc.function.name:
                                tool_calls[idx]["name"] = tc.function.name
                            if tc.function.arguments:
                                tool_calls[idx]["arguments_json"] += tc.function.arguments

            ordered_tool_calls = [tool_calls[i] for i in sorted(tool_calls.keys())]

            if ordered_tool_calls and (finish_reason == "tool_calls" or finish_reason is None):
                parsed_calls = []
                for i, tc in enumerate(ordered_tool_calls):
                    call_id = tc["id"] or f"call_{tool_rounds}_{i}"
                    try:
                        tool_input = json.loads(tc["arguments_json"]) if tc["arguments_json"] else {}
                    except json.JSONDecodeError:
                        tool_input = {}
                    parsed_calls.append({
                        "id": call_id,
                        "name": tc["name"],
                        "input": tool_input,
                        "arguments_json": tc["arguments_json"] or "{}",
                    })

                messages.append({
                    "role": "assistant",
                    "content": collected_text or "",
                    "tool_calls": [
                        {
                            "id": pc["id"],
                            "type": "function",
                            "function": {
                                "name": pc["name"],
                                "arguments": pc["arguments_json"],
                            },
                        }
                        for pc in parsed_calls
                    ],
                })

                for pc in parsed_calls:
                    yield _sse({
                        "type": "tool_use",
                        "name": pc["name"],
                        "input": pc["input"],
                    })
                    result = execute_tool(pc["name"], pc["input"])
                    yield _sse({
                        "type": "tool_result",
                        "name": pc["name"],
                        "result": _truncate_result(result),
                    })
                    messages.append({
                        "role": "tool",
                        "tool_call_id": pc["id"],
                        "content": json.dumps(result, default=str),
                    })

                tool_rounds += 1
                continue

            if collected_text:
                messages.append({"role": "assistant", "content": collected_text})
            break

        yield _sse({"type": "done"})

    except Exception as e:
        msg = str(e)
        lower_msg = msg.lower()
        if "authentication" in lower_msg or "invalid api key" in lower_msg:
            yield _sse({"type": "error", "message": "Invalid API key. Please check your OpenAI API key and try again."})
        elif "rate limit" in lower_msg or "429" in lower_msg:
            yield _sse({"type": "error", "message": "Rate limit exceeded. Please wait a moment and try again."})
        elif "connection" in lower_msg or "connect" in lower_msg:
            yield _sse({"type": "error", "message": "Could not connect to the OpenAI API. Please check your network connection."})
        else:
            logger.exception("OpenAI chat error")
            yield _sse({"type": "error", "message": f"An error occurred: {msg}"})


def _sse(data):
    """Format a dict as an SSE data line."""
    return f"data: {json.dumps(data, default=str)}\n\n"


def _truncate_result(result, max_items=5):
    """Truncate tool results for UI display (not for Claude — Claude gets the full result)."""
    if isinstance(result, dict) and "results" in result:
        truncated = dict(result)
        if len(truncated["results"]) > max_items:
            truncated["results"] = truncated["results"][:max_items]
            truncated["truncated"] = True
        return truncated
    return result
