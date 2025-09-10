package main

import (
	"bytes"
	"encoding/json"
	"flag"
	"fmt"
	"go/ast"
	"go/parser"
	"go/printer"
	"go/token"
	"os"
	"path/filepath"
	"strings"
)

type Node struct {
	ID        string   `json:"id"`
	Label     string   `json:"label"`
	Type      string   `json:"type"`
	File      string   `json:"file"`
	Line      int      `json:"line"`
	Params    []string `json:"params,omitempty"`
	Results   []string `json:"results,omitempty"`
	InDegree  int      `json:"in_degree"`
	OutDegree int      `json:"out_degree"`
}

type Edge struct {
	Source string `json:"source"`
	Target string `json:"target"`
	Type   string `json:"type"`
	File   string `json:"file"`
	Line   int    `json:"line"`
}

type Graph struct {
	Nodes []Node         `json:"nodes"`
	Edges []Edge         `json:"edges"`
	Stats map[string]int `json:"stats"`
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(flag.CommandLine.Output(), "Usage: go run go_code_graph.go [path]\n")
	}
	flag.Parse()
	path := "."
	if flag.NArg() > 0 {
		path = flag.Arg(0)
	}

	g := analyze(path)
	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(g); err != nil {
		fmt.Fprintln(os.Stderr, "encode error:", err)
		os.Exit(1)
	}
}

func analyze(root string) Graph {
	nodes := map[string]*Node{}
	edges := []Edge{}
	fs := token.NewFileSet()

	filepath.WalkDir(root, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			switch d.Name() {
			case ".git", "vendor", "node_modules":
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") {
			return nil
		}
		rel, _ := filepath.Rel(root, path)
		file, err := parser.ParseFile(fs, path, nil, 0)
		if err != nil {
			return nil
		}
		pkg := file.Name.Name

		// structs and funcs
		for _, decl := range file.Decls {
			switch d := decl.(type) {
			case *ast.GenDecl:
				if d.Tok == token.TYPE {
					for _, spec := range d.Specs {
						ts, ok := spec.(*ast.TypeSpec)
						if !ok {
							continue
						}
						if _, ok := ts.Type.(*ast.StructType); ok {
							id := pkg + "." + ts.Name.Name
							pos := fs.Position(ts.Pos())
							nodes[id] = &Node{ID: id, Label: ts.Name.Name, Type: "struct", File: rel, Line: pos.Line}
						}
					}
				}
			case *ast.FuncDecl:
				id := funcID(pkg, d)
				pos := fs.Position(d.Pos())
				ntype := "function"
				if d.Recv != nil {
					ntype = "method"
				}
				params := fieldTypes(d.Type.Params, fs)
				results := fieldTypes(d.Type.Results, fs)
				nodes[id] = &Node{ID: id, Label: d.Name.Name, Type: ntype, File: rel, Line: pos.Line, Params: params, Results: results}

				if d.Body != nil {
					ast.Inspect(d.Body, func(n ast.Node) bool {
						call, ok := n.(*ast.CallExpr)
						if !ok {
							return true
						}
						target := callName(pkg, call)
						if target == "" {
							return true
						}
						p := fs.Position(call.Pos())
						edges = append(edges, Edge{Source: id, Target: target, Type: "calls", File: rel, Line: p.Line})
						nodes[id].OutDegree++
						if tgt, ok := nodes[target]; ok {
							tgt.InDegree++
						}
						return true
					})
				}
			}
		}
		return nil
	})

	// build lists and stats
	nodeList := []Node{}
	stats := map[string]int{"structs": 0, "functions": 0, "methods": 0}
	for _, n := range nodes {
		nodeList = append(nodeList, *n)
		stats[n.Type+"s"]++
	}
	stats["total_nodes"] = len(nodeList)
	stats["total_edges"] = len(edges)

	return Graph{Nodes: nodeList, Edges: edges, Stats: stats}
}

func funcID(pkg string, f *ast.FuncDecl) string {
	if f.Recv == nil {
		return pkg + "." + f.Name.Name
	}
	recv := "unknown"
	if len(f.Recv.List) > 0 {
		switch t := f.Recv.List[0].Type.(type) {
		case *ast.Ident:
			recv = t.Name
		case *ast.StarExpr:
			if id, ok := t.X.(*ast.Ident); ok {
				recv = id.Name
			}
		}
	}
	return pkg + "." + recv + "." + f.Name.Name
}

func callName(pkg string, c *ast.CallExpr) string {
	switch fn := c.Fun.(type) {
	case *ast.Ident:
		return pkg + "." + fn.Name
	case *ast.SelectorExpr:
		if id, ok := fn.X.(*ast.Ident); ok {
			return id.Name + "." + fn.Sel.Name
		}
		return "*." + fn.Sel.Name
	default:
		return ""
	}
}

func fieldTypes(fl *ast.FieldList, fs *token.FileSet) []string {
	if fl == nil {
		return nil
	}
	types := []string{}
	for _, f := range fl.List {
		types = append(types, exprString(f.Type, fs))
	}
	return types
}

func exprString(e ast.Expr, fs *token.FileSet) string {
	var buf bytes.Buffer
	printer.Fprint(&buf, fs, e)
	return buf.String()
}
