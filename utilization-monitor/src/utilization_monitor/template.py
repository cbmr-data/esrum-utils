from __future__ import annotations

import itertools
import json
from collections.abc import Iterator
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    import altair as alt


class Unspecified:
    pass


UNSPECIFIED = Unspecified()


class Row:
    def __init__(
        self,
        *args: None | float | str | Cell,
        cls: str | None = None,
        data: dict[str, str] | None = None,
    ) -> None:
        self.cls = cls
        self.data = {} if data is None else data
        self.values = args

    def __iter__(self) -> Iterator[None | float | str | Cell]:
        return iter(self.values)


class Cell:
    def __init__(
        self,
        value: None | float | str,
        *,
        sort_data: Unspecified | float | str | None = UNSPECIFIED,
        raw_data: Unspecified | float | str | None = UNSPECIFIED,
        shading: float | Literal["DYNAMIC"] = 0,
    ) -> None:
        self.value = value
        self.shading = (
            max(0, round(shading, 3)) if isinstance(shading, float) else shading
        )

        self.sort_data = sort_data
        self.raw_data = raw_data

    @property
    def has_sort_data(self) -> bool:
        return not isinstance(self.sort_data, Unspecified)

    def __str__(self) -> str:
        return "" if self.value is None else str(self.value)


class Section:
    _title: str | None
    _paragraphs: list[str]
    _running_id: int = 0

    def __init__(self) -> None:
        self._title = None
        self._paragraphs = []

    def set_title(self, title: str) -> Section:
        self._title = title
        return self

    def add_subtitle(self, title: str) -> Section:
        self._paragraphs.append(f"        <h2>{title}</h2>")

        return self

    def add_chart(
        self,
        chart: alt.Chart | alt.FacetChart | alt.LayerChart,
        caption: str | None = None,
    ) -> Section:
        spec: str = chart.to_json(indent=None, format="vega")

        Section._running_id += 1
        self._paragraphs.append(
            _TEMPLATE_ALTAIR.format(
                id=Section._running_id,
                spec=spec,
            )
        )

        if caption is not None:
            self._paragraphs.append(f"      <figcaption>{caption}</figcaption>")

        return self

    def add_table(
        self,
        rows: list[Row] | list[list[str | int | None | float | Cell]],
        columns: list[str | None],
        table_id: str | None = None,
    ) -> Section:
        self._update_shading(rows)
        lines: list[str] = []
        add = lines.append

        table_id = "" if table_id is None else f'id="{table_id}"'
        classes = "sortable pure-table io-table pure-table-striped"
        add(f'      <table class="{classes}" {table_id}>')
        if columns:
            add("        <thead>")
            add("          <tr>")
            for idx, name in enumerate(columns):
                attrs = ""
                if name is None:
                    if idx:
                        name = ""
                    else:
                        name = (
                            '<input type="checkbox" onchange="toggle_all(this)" '
                            "checked />"
                        )
                    attrs = ' class="no-sort"'

                add(f"            <th{attrs}>{name}</th>")
            add("          </tr>")
            add("        </thead>")
        add("        <tbody>")
        for row in rows:
            row_attrs: list[str] = []
            if isinstance(row, Row):
                for key, value in row.data.items():
                    row_attrs.append(f"data-{key}={value!r}")

                if row.cls is not None:
                    row_attrs.append(f"class={row.cls}")

            add(f"          <tr {' ' .join(row_attrs)}>")
            for value in row:
                attrs = ""
                if (
                    isinstance(value, Cell)
                    and isinstance(value.shading, float)
                    and value.shading > 0
                ):
                    pct = round(100 * value.shading, 1)
                    attrs = f" class='percent' style='background-size: {pct}% 100%'"
                elif value is None:
                    value = ""

                if isinstance(value, Cell) and value.has_sort_data:
                    attrs = f'{attrs} data-sort="{value.sort_data}" '

                add(f"            <td{attrs}>{value}</td>")
            add("          </tr>")
        add("        </tbody>")
        add("      </table>")

        self._paragraphs.append("\n".join(lines))

        return self

    def add_paragraph(self, *text: str) -> Section:
        self._paragraphs.append("      <p>{}</p>".format(" ".join(text)))

        return self

    def render(self) -> str:
        elements: list[str] = ['<div class="section">']
        if self._title is not None:
            elements.append(_TEMPLATE_TITLE.format(title=self._title))

        elements.extend(self._paragraphs)
        elements.append("</div>")

        return "\n".join(elements)

    def _update_shading(
        self,
        rows: list[Row] | list[list[str | float | None | Cell]],
    ) -> None:
        for column in itertools.zip_longest(*rows):
            values: list[float] = []
            for it in column:
                if isinstance(it, Cell):
                    if it.shading == "DYNAMIC" and isinstance(it.value, int | float):
                        values.append(it.value)
                elif it is not None:
                    break
            else:
                max_value = max(values, default=1)
                for it in column:
                    if (
                        isinstance(it, Cell)
                        and isinstance(it.value, int | float)
                        and it.shading == "DYNAMIC"
                    ):
                        it.shading = it.value / max_value


class Report:
    _title: str
    _sections: list[Section]

    def __init__(self, title: str) -> None:
        self._title = title
        self._sections = []

    def add(self) -> Section:
        s = Section()
        self._sections.append(s)
        return s

    def render(self) -> str:
        return (
            _TEMPLATE_DOC.strip("\r\n")
            .replace("{title}", self._title)
            .replace("{external}", self._render_external())
            .replace(
                "{body}",
                "\n\n".join(s.render().rstrip("\r\n") for s in self._sections),
            )
        )

    def _render_external(self) -> str:
        external: list[tuple[str, str]] = [
            (
                "https://cdn.jsdelivr.net/npm/purecss@3.0.0/build/pure-min.css",
                "X38yfunGUhNzHpBaEBsWLO+A0HDYOQi8ufWDkZ0k9e0eXz/tH3II7uKZ9msv++Ls",
            ),
            (
                "https://cdn.jsdelivr.net/gh/tofsjonas/sortable@3.0.0/sortable-base.min.css",
                "RMPvgKdhV7JWj5RH7yq5bQgwAt02lpUEAdzaUPj4RLA9BdifNNiI1gtIobFLfeuO",
            ),
            (
                "https://cdn.jsdelivr.net/gh/tofsjonas/sortable@3.2.2/sortable.min.js",
                "Ui7TCZUUp8xuvYhwek30kUmzgl+cbbS0TWhUdE7J3/dF/O7kuF0wd6ZlLZ3KhLVn",
            ),
            (
                "https://cdn.jsdelivr.net/npm/vega@5.25.0/build/vega.min.js",
                "iY3zZAtrtgjJoD8rliThCLEeLUYo8aSNWYQkL+Jaa3KQEAACPnaw/lQIRrFbPCsj",
            ),
            (
                "https://cdn.jsdelivr.net/npm/vega-lite@5.20.1/build/vega-lite.min.js",
                "OEZvwKj/3vVASzQVgbPCDiMTqVgwOEKv4n4SPEX2WQk7ZEaK0DBqL65qncIVWa16",
            ),
            (
                "https://cdn.jsdelivr.net/npm/vega-embed@6.22.2/build/vega-embed.min.js",
                "EA8k5FkiwPXfiSQeH8xlNaljrtD6qj7T49n8VoweOD7Tlm/DHHaoKLDbtJ+8ly5+",
            ),
        ]

        lines: list[str] = []
        for url, sha384 in external:
            if url.endswith(".css"):
                lines.append(_TEMPLATE_CSS.format(url=url, hash=sha384))
            elif url.endswith(".js"):
                lines.append(_TEMPLATE_JS.format(url=url, hash=sha384))
            else:
                raise NotImplementedError(url)

        return "".join(lines)


_TEMPLATE_DOC = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8">
  <meta name="viewport" content="width=device-width, initial-scale=1.0">
  <meta http-equiv="X-UA-Compatible" content="ie=edge">
  <title>{title}</title>
  {external}
    <style type='text/css'>
      body {
        background-color: #E3E2DE;
      }

      div#layout {
          max-width: 1280px;
          margin-left: auto;
          margin-right: auto;
          font-size: smaller;
      }

      div.title {
          background-color: #8C9CC0;
          margin: -10px !important;
          text-align: center;
          border-radius: 5px;
      }

      div.title>h1,
      div.title>h2 {
          padding: 5px;
      }

      h5 {
          margin-bottom: 2px;
          margin-left: 6px;
      }

      .pure-table {
          margin-left: 1em;
          text-align: right;
      }

      .pure-table thead>tr>th {
          font-weight: bold;
          background-color: #C4CCDB;
      }

      .pure-table tbody>tr:nth-child(even)>td.percent {
        background-image: linear-gradient(to right, #C4CCDB, #C4CCDB);
        background-repeat: no-repeat;
        background-position: 100% 100%; /* right aligned */
      }

      .pure-table tbody>tr:nth-child(odd)>td.percent {
        background-image: linear-gradient(to right, #D4DCEB, #D4DCEB);
        background-repeat: no-repeat;
        background-position: 100% 100%; /* right aligned */
      }

      .section {
          background-color: #FFF;
          border-radius: 5px;
          margin-bottom: 10px;
          padding: 10px;
          padding-top: 0px;
      }

      .epilogue,
      .note {
          color: #777;
          font-size: small;
          padding-top: 10px;
      }
    </style>
  <script>
    let QC_SAMPLES = {};
    let QC_EXPERIMENTS= {};

    function on_click_checkbox(chk) {
      if (chk.dataset["experiment"]) {
        QC_EXPERIMENTS[chk.dataset["experiment"]] = chk.checked;
      } else if (chk.dataset["sample"]) {
        QC_SAMPLES[chk.dataset["sample"]] = chk.checked;
      }

      update_samples("sample");
      update_samples("mapping");
    }

    function initialize() {
      for (let elem of document.getElementsByClassName("experiment")) {
        for (let child of elem.getElementsByTagName("input")) {
          QC_EXPERIMENTS[elem.dataset["experiment"]] = child.checked;
        }
      }

      for (let elem of document.getElementsByClassName("sample")) {
        for (let child of elem.getElementsByTagName("input")) {
          QC_SAMPLES[elem.dataset["sample"]] = child.checked;
        }
      }

      update_samples("sample");
        update_samples("mapping");
    }

    function update_samples(className) {
      for (let elem of document.getElementsByClassName(className)) {
        var sample = elem.dataset["sample"];
        var visible = className === "mapping" ? QC_SAMPLES[sample] : true;
        if (visible) {
          var experiment_visible = false;
          for (let experiment of elem.dataset["experiments"].split(" ")) {
            if (QC_EXPERIMENTS[experiment]) {
              experiment_visible = true;
              break;
            }
          }

          visible = visible && experiment_visible;
        }

        if (visible) {
          elem.style.removeProperty("display");
        } else {
          elem.style["display"] = "none";
        }
      }
    }

    function toggle_all(elem) {
      let checked = elem.checked;
      while (elem && elem.tagName !== "TABLE") {
        elem = elem.parentElement;
      }

      for (let child of elem.getElementsByTagName("INPUT")) {
        if (child.checked !== checked) {
          child.checked = checked;
          child.onchange(child);
        }
      }
    }
  </script>
</head>
<body onload="initialize()">
  <main>
    <div id='layout'>
      <div class="title">
        <h1>{title}</h1>
      </div>
{body}
    </div>
  </main>
</body>
</html>
"""

# sha384 hashes calculated as
#   $ openssl dgst -sha384 -binary ${filename} | openssl base64

_TEMPLATE_CSS = """   <link
    rel="stylesheet"
    href="{url}"
    integrity="sha384-{hash}"
    crossorigin="anonymous">
"""

_TEMPLATE_JS = """   <script
    src="{url}"
    integrity="sha384-{hash}"
    crossorigin="anonymous"></script>
"""

_TEMPLATE_TITLE = """
      <div class="title">
        <h2>{title}</h2>
      </div>
"""

_TEMPLATE_ALTAIR = """
      <div id="vega{id}"></div>
      <script type="text/javascript">
          var spec = {spec};
          vegaEmbed("#vega{id}", spec);
      </script>
"""
