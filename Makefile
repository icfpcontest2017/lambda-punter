LIBS = "yojson ocamlgraph lwt lwt.unix str"

OCB_FLAGS = -use-ocamlfind -pkgs $(LIBS) -tag bin_annot
OCB = ocamlbuild $(OCB_FLAGS)

all: 	native

clean:
	$(OCB) -clean
	rm -f lampunt

native:
	$(OCB) lp.native
	cp lp.native lampunt

profile:
	$(OCB) -tag profile lp.native
	cp lp.native lampunt

debug:
	$(OCB) -tag debug lp.native
	cp lp.native lampunt

.PHONY: all clean native profile debug
