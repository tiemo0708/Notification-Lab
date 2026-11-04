# Diagram sources and licenses

- Generator/template: [caru-ini/stack-diagram](https://github.com/caru-ini/stack-diagram), commit `1ed53e3e9525364ae5d82e0430f2bb3f7ef4c838`, copyright 2026 caru-ini.
  The adapted template and generated HTML retain the [MIT notice](licenses/stack-diagram-MIT.txt).
- Embedded Geist and JetBrains Mono fonts retain their SIL Open Font License 1.1 notices:
  [Geist](licenses/OFL-Geist.txt), [JetBrains Mono](licenses/OFL-JetBrainsMono.txt).
- Spring, MySQL and Redis logos were fetched through [svgl](https://svgl.app/).
  The Apache Kafka logo was fetched through [gilbarbara/logos](https://github.com/gilbarbara/logos).
  Source URLs and file metadata are preserved in [icons-manifest.json](icons-manifest.json).
  Brand marks are used to identify the corresponding technologies, remain their owners' trademarks, and imply no endorsement.
  Their colors and shapes have not been changed.
- Non-brand Batch/Relay/Worker/Mock pictograms are from [Lucide](https://lucide.dev/),
  with the [upstream license notice](licenses/Lucide-ISC.txt). The generator applies an ink color to these abstract pictograms.

The standalone HTML embeds logos and fonts as data URIs. PNGs are browser renders of that same HTML.
The editable template uses placeholders; use the build instructions in [ARCHITECTURE.md](../ARCHITECTURE.md) to regenerate it.
