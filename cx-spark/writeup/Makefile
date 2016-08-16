# I'm unclear on how to setup the deps for TeX and Bib files, so just mark it as .PHONY for now:
.PHONY: heromsi.pdf
all: ipdps.tex 
	pdflatex ipdps.tex
	bibtex ipdps
	pdflatex ipdps.tex
	pdflatex ipdps.tex

heromsi.pdf:
	pdflatex heromsi
	bibtex heromsi
	pdflatex heromsi
	pdflatex heromsi

.PHONY: endless
endless:
	ls *.tex *.bib | entr make heromsi.pdf
