FROM opensearchproject/opensearch:latest

# Plugins for japanese text.
# Reference: https://subro.mokuren.ne.jp/0930.html
RUN /usr/share/opensearch/bin/opensearch-plugin install --batch analysis-kuromoji analysis-icu
