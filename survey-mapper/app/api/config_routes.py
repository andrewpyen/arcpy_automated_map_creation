from __future__ import annotations

import html
import json
from typing import Any, Dict, List, Optional, Annotated, get_origin
from fastapi import FastAPI
from fastapi.openapi.utils import get_openapi

from fastapi import APIRouter, Query, Request
from fastapi.responses import HTMLResponse
from pydantic import BaseModel, ValidationError

from app.config_loading.settings import get_settings, refresh_settings
from app.config_loading.config_loader import get_config, save_config, AppConfig

config_router = APIRouter(prefix="/config", tags=["Configuration"])

# Survey type as plain str - validate and render options from settings at runtime
SurveyTypeParam = Annotated[
    str | None,
    Query(
        description="Survey type - pick from the dropdown",
        json_schema_extra={"x-dynamic-enum": "survey_types"},
    ),
]
# Docs wiring: add a visible link in /docs to /config/edit ---
def wire_dynamic_enums_and_links(app: FastAPI) -> None:
    # reset cached schema before swapping generator
    app.openapi_schema = None
    def custom_openapi():
        if app.openapi_schema:
            return app.openapi_schema
        schema = get_openapi(
            title=app.title or "API",
            version=getattr(app, "version", "0.1.0"),
            description=(
                (app.description or "")
                + "\n\n### Configuration File Editor\n"
                + "- ## [Open the Editor](/config/edit)"),
                routes=app.routes,
        )

        # Build sources
        from app.config_loading.zip_registry_single import list_zip_files_single
        live_survey_types = list(get_settings().SURVEY_TYPES or [])
        live_zip_files = list_zip_files_single()

        # Builds new survey and zip file lists dynamically
        sources = {
            "survey_types": live_survey_types,
            "zip_files": live_zip_files,
        }

        # Walk paths and insert enum values for marked parameters
        for _path, item in schema.get("paths", {}).items():
            for _method, _operation in item.items():
                if not isinstance(_operation, dict):
                    continue
                for param in _operation.get("parameters", []):
                    sch = param.get("schema", {})
                    dyn_key = sch.get("x-dynamic-enum")
                    if dyn_key and dyn_key in sources:
                        values = sources[dyn_key]
                        sch["enum"] = values
                        if "default" not in sch and values:
                            sch["default"] = values[0]

        app.openapi_schema = schema
        return app.openapi_schema
    app.openapi = custom_openapi

# ----------------------------
# Helper - very small HTML kit
# ----------------------------
def _h(tag: str, attrs: Dict[str, str] | None = None, inner: str = "") -> str:
    attrs = attrs or {}
    attr_str = "".join(f' {k}="{html.escape(v, quote=True)}"' for k, v in attrs.items() if v is not None)
    return f"<{tag}{attr_str}>{inner}</{tag}>"

def _input_text(name: str, value: str, label: str, desc: str = "") -> str:
    field = f'<input name="{html.escape(name)}" type="text" value="{html.escape(value)}" class="text" />'
    return _wrap_field(label, field, desc)

def _input_number(name: str, value: str, label: str, desc: str = "", step: str = "1") -> str:
    field = f'<input name="{html.escape(name)}" type="number" step="{step}" value="{html.escape(value)}" class="text" />'
    return _wrap_field(label, field, desc)

def _input_checkbox(name: str, checked: bool, label: str, desc: str = "") -> str:
    chk = "checked" if checked else ""
    field = f'<input name="{html.escape(name)}" type="checkbox" value="true" {chk} />'
    return _wrap_field(label, field, desc)

def _select(name: str, value: str, options: List[str], label: str, desc: str = "") -> str:
    opts = []
    for o in options:
        sel = ' selected="selected"' if str(o) == str(value) else ""
        opts.append(f'<option value="{html.escape(str(o))}"{sel}>{html.escape(str(o))}</option>')
    field = f'<select name="{html.escape(name)}" class="text">{"".join(opts)}</select>'
    return _wrap_field(label, field, desc)

def _textarea_json(name: str, value_obj: Any, label: str, desc: str = "") -> str:
    # Pretty JSON so it is readable and editable
    try:
        pretty = json.dumps(value_obj, indent=2)
    except Exception:
        # Fallback to raw str
        pretty = str(value_obj)
    field = f'<textarea name="{html.escape(name)}" rows="10" class="code">{html.escape(pretty)}</textarea>'
    help_extra = " Must be valid JSON."
    return _wrap_field(label, field, (desc + help_extra).strip())

def _wrap_field(label: str, control_html: str, desc: str) -> str:
    lab = f"<label>{html.escape(label)}</label>"
    help_ = f'<div class="help">{html.escape(desc)}</div>' if desc else ""
    return f'<div class="field">{lab}{control_html}{help_}</div>'

def _base_css() -> str:
    return """
    <style>
      body { font-family: system-ui, -apple-system, Segoe UI, Roboto, Arial, sans-serif; margin: 24px; }
      .container { max-width: 980px; margin: 0 auto; }
      h1 { margin: 0 0 8px 0; }
      .bar { display: flex; gap: 8px; align-items: center; margin: 12px 0 20px; }
      label { font-weight: 600; display: block; margin-bottom: 6px; }
      .field { border: 1px solid #e5e5e5; padding: 12px; border-radius: 8px; margin-bottom: 12px; }
      .text, select { width: 100%; padding: 8px; box-sizing: border-box; }
      textarea.code { width: 100%; font-family: ui-monospace, SFMono-Regular, Menlo, Consolas, monospace; }
      .help { color: #555; font-size: 12px; margin-top: 6px; }
      .msg { padding: 10px 12px; border-radius: 8px; margin-bottom: 12px; }
      .ok { background: #eef8ee; border: 1px solid #cfe9cf; color: #0a6a0a; }
      .err { background: #fff3f3; border: 1px solid #f0c2c2; color: #8a1010; white-space: pre-wrap; }
      .actions { display: flex; gap: 8px; }
      button { padding: 8px 14px; cursor: pointer; }
      .footer { color: #666; font-size: 12px; margin-top: 12px; }
    </style>
    """

# ----------------------------
# Form rendering and parsing
# ----------------------------
def _schema_enums(model_cls: type[BaseModel]) -> Dict[str, List[str]]:
    """Get enum choices from the model JSON schema for dropdowns."""
    props = (model_cls.model_json_schema() or {}).get("properties", {})
    enums: Dict[str, List[str]] = {}
    for k, v in props.items():
        if isinstance(v, dict) and "enum" in v:
            enums[k] = list(v["enum"])
    return enums

def _is_bool(annotation: Any) -> bool:
    return annotation is bool

def _is_int(annotation: Any) -> bool:
    return annotation is int

def _is_float(annotation: Any) -> bool:
    return annotation is float

def _is_json_like(annotation: Any) -> bool:
    """Return True for Dict, List, arbitrary objects, or nested models."""
    origin = get_origin(annotation)
    if origin in (list, dict, tuple, set):
        return True
    if isinstance(annotation, type) and issubclass(annotation, BaseModel):
        return True
    # object annotations and untyped fallbacks
    return annotation in (dict, list, tuple, set, object, Any)

def _render_form(cfg: BaseModel, model_cls: type[BaseModel], survey_type: SurveyTypeParam, message: str = "", err: str = "") -> str:
    enums = _schema_enums(model_cls)
    parts: List[str] = []

    # survey type selector on top
    types = list(get_settings().SURVEY_TYPES or [])
    selector = ""
    if types:
        opts = "".join(
            f'<option value="{html.escape(t)}"{" selected" if t == survey_type else ""}>{html.escape(t)}</option>'
            for t in types
        )
        selector = f"""
        <form method="get" action="/config/edit" class="bar">
          <label for="stype">Survey Type</label>
          <select name="survey_type" id="stype">{opts}</select>
          <button type="submit">Load</button>
          <a href="/config/edit">Reset</a>
        </form>
        """

    if message:
        parts.append(f'<div class="msg ok">{html.escape(message)}</div>')
    if err:
        parts.append(f'<div class="msg err">{html.escape(err)}</div>')

    # main form
    parts.append(f'<form method="post" action="/config/edit?survey_type={html.escape(survey_type)}">')

    # iterate model fields
    for name, field in model_cls.model_fields.items():
        label = field.title or name
        desc = (field.description or "").strip()
        value = getattr(cfg, name, None)

        # enum dropdown
        if name in enums:
            parts.append(_select(name, "" if value is None else str(value), enums[name], label, desc))
            continue

        ann = field.annotation
        # booleans
        if _is_bool(ann):
            parts.append(_input_checkbox(name, bool(value), label, desc))
            continue

        # ints
        if _is_int(ann):
            parts.append(_input_number(name, "" if value is None else str(value), label, desc, step="1"))
            continue

        # floats
        if _is_float(ann):
            parts.append(_input_number(name, "" if value is None else str(value), label, desc, step="any"))
            continue

        # complex structures - present as JSON textarea
        if _is_json_like(ann):
            parts.append(_textarea_json(name, value, label, desc))
            continue

        # default - text
        parts.append(_input_text(name, "" if value is None else str(value), label, desc))

    parts.append('<div class="actions"><button type="submit">Save</button></div>')
    parts.append("</form>")

    footer = '<div class="footer">Edits are validated with Pydantic on save.</div>'
    body = "\n".join(parts) + footer

    html_page = f"""<!doctype html>
<html><head><meta charset="utf-8" />
<title>Survey Config Editor</title>{_base_css()}</head>
<body>
  <div class="container">
    <h1>Survey Config Editor</h1>
    <p>Select a survey type, edit values, then Save. Lists and objects are edited as JSON.</p>
    {selector}
    {body}
  </div>
</body></html>"""
    return html_page

def _coerce_field(raw: str, ann: Any) -> Any:
    """Coerce a single form field string into the annotated type."""
    if _is_bool(ann):
        # checkbox only posts when true
        return raw.lower() in ("true", "1", "on", "yes")
    if _is_int(ann):
        return int(raw)
    if _is_float(ann):
        return float(raw)
    if _is_json_like(ann):
        # require JSON for complex fields
        return json.loads(raw) if raw.strip() else None
    return raw

# ----------------------------
# Routes
# ----------------------------
@config_router.get("/edit", 
                   summary="Edit The Configuration HTML Page",
                   include_in_schema=False, # This hides this end point from the Swagger UI since it's only used in the HTML config editor page
                   response_class=HTMLResponse,
                   tags=["Configuration"])
def edit_config_page(survey_type: Optional[SurveyTypeParam] = None):
    # Pick a default survey type if none selected
    stype = survey_type or (get_settings().SURVEY_TYPES[0] if get_settings().SURVEY_TYPES else "")
    if not stype:
        # No types configured - render an empty page with a message
        empty_page = f"""<!doctype html><html><head><meta charset="utf-8" />
        <title>Survey Config Editor</title>{_base_css()}</head>
        <body><div class="container">
        <h1>Survey Config Editor</h1>
        <div class="msg err">No survey types are configured. Set get_settings().SURVEY_TYPES to a non-empty list.</div>
        </div></body></html>"""
        return HTMLResponse(empty_page)

    try:
        cfg = get_config(stype)
    except Exception as e:
        # Render page with the error message
        dummy = AppConfig.model_validate({
            # minimal placeholders so the renderer has values
            "projectName": "",
            "surveyType": stype,
            "outputDirectory": "",
            "lutassettypes": {},
            "gridzones": {},
            "feature_classes_to_clip": [],
        })
        page = _render_form(dummy, AppConfig, stype, message="", err=str(e))
        return HTMLResponse(page, status_code=200)

    page = _render_form(cfg, AppConfig, stype)
    return HTMLResponse(page)

@config_router.post("/edit", 
                   summary="Edit The Configuration HTML Page",
                   include_in_schema=False, # This hides this end point from the Swagger UI since it's only used in the HTML config editor page
                   response_class=HTMLResponse,
                   tags=["Configuration"])
async def save_config_page(request: Request, survey_type: SurveyTypeParam):
    """Accepts form post, validates with AppConfig, saves, then re-renders."""
    form = await request.form()
    # Build a plain dict of values coerced to the field types
    incoming: Dict[str, Any] = {}
    errors: List[str] = []

    model_cls = AppConfig

    for name, field in model_cls.model_fields.items():
        ann = field.annotation
        raw = form.get(name)

        if raw is None:
            # For checkboxes, missing means false
            if _is_bool(ann):
                incoming[name] = False
                continue
            # Leave missing optional values out
            continue

        # Note: surveyType should match the selected query param
        if name == "surveyType":
            incoming[name] = survey_type
            continue

        try:
            incoming[name] = _coerce_field(str(raw), ann)
        except Exception as ex:
            errors.append(f"{name}: {ex}")

    # Ensure surveyType consistency
    incoming["surveyType"] = survey_type

    if errors:
        # If parse errors, re-render with errors
        try:
            # Try to fill current values for display even if invalid
            current_cfg = get_config(survey_type)
        except Exception:
            # Build a minimal instance to render
            current_cfg = AppConfig.model_validate({
                "projectName": incoming.get("projectName", ""),
                "surveyType": survey_type,
                "outputDirectory": incoming.get("outputDirectory", ""),
                "lutassettypes": incoming.get("lutassettypes", {}),
                "gridzones": incoming.get("gridzones", {}),
                "feature_classes_to_clip": incoming.get("feature_classes_to_clip", []),
            })
        page = _render_form(current_cfg, model_cls, survey_type, err="; ".join(errors))
        return HTMLResponse(page, status_code=400)

    # Validate against Pydantic
    try:
        new_cfg = model_cls.model_validate(incoming)
    except ValidationError as ve:
        # Show validation errors inline
        page = _render_form(
            # make a best effort to show what the user posted
            model_cls.model_construct(**{**incoming}),
            model_cls,
            survey_type,
            err=str(ve)
        )
        return HTMLResponse(page, status_code=422)

    # Persist and re-render success
    saved_path = save_config(survey_type, new_cfg)
    page = _render_form(new_cfg, model_cls, survey_type, message=f"Saved to {saved_path}")
    return HTMLResponse(page)
