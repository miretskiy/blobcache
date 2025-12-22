package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/miretskiy/blobcache"
)

func main() {
	// Define flags
	recover := flag.Bool("recover", false, "Recover index by scanning segment files and removing corrupt segments")
	path := flag.String("path", "", "Path to blobcache directory (required)")
	flag.Parse()

	// Validate required flags
	if *path == "" {
		fmt.Fprintln(os.Stderr, "Error: --path is required")
		flag.Usage()
		os.Exit(1)
	}

	// Check if recover flag is specified
	if !*recover {
		fmt.Fprintln(os.Stderr, "Error: --recover flag must be specified")
		fmt.Fprintln(os.Stderr, "\nUsage: blobcache-recover --recover --path=/path/to/cache")
		fmt.Fprintln(os.Stderr, "\nThis tool rebuilds the cache index by scanning segment files.")
		fmt.Fprintln(os.Stderr, "It will remove any corrupt segment files or segments without valid footers.")
		os.Exit(1)
	}

	// Run recovery
	fmt.Printf("Starting recovery for cache at: %s\n", *path)
	fmt.Println("WARNING: This will remove corrupt segment files and rebuild the index.")
	fmt.Println()

	cache, err := blobcache.RecoverIndex(*path)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Recovery failed: %v\n", err)
		os.Exit(1)
	}

	// Close the cache
	if err := cache.Close(); err != nil {
		fmt.Fprintf(os.Stderr, "Warning: failed to close cache: %v\n", err)
	}

	fmt.Println("\nRecovery completed successfully!")
	fmt.Println("You can now use the cache normally.")
}
