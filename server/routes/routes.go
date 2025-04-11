package routes

import (
	"bytes"
	"db/database"
	"db/dbcli"
	cli "db/dbcli"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/gofiber/fiber/v2"
	"github.com/google/uuid"
)

var openDBs = map[string]*database.Database{}

func basePath(dbID string) string {
	return filepath.Join(".", "files", dbID)
}

func getDB(dbID string, createIfMissing bool) (*database.Database, string, error) {
	if db, ok := openDBs[dbID]; ok {
		return db, dbID, nil
	}

	db, err := database.LoadDatabase(basePath(dbID))
	if err != nil {
		if !createIfMissing {
			return nil, "", err
		}
		dbUUID, err := uuid.NewRandom()
		if err != nil {
			log.Fatalf("failed to generate uuid: %v", err)
		}
		dbSuffix := strings.Split(dbUUID.String(), "-")[0]
		dbID = fmt.Sprintf("db_%s", dbSuffix)
		db, err = database.NewDatabase(basePath(dbID), dbID)
		if err != nil {
			return nil, dbID, err
		}
	}
	openDBs[dbID] = db
	return db, dbID, nil
}

func runCLI(args []string) (string, error) {
	var out, errBuf bytes.Buffer
	cli.RootCmd.SetOut(&out)
	cli.RootCmd.SetErr(&errBuf)
	cli.RootCmd.SetArgs(args)
	err := cli.RootCmd.Execute()
	return out.String() + errBuf.String(), err
}

func SetupRoutes(router fiber.Router) {
	router.Get("/databases", func(c *fiber.Ctx) error {
		dbs, err := database.ListDatabases("./files")
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{
				"error": err.Error(),
			})
		}
		return c.JSON(fiber.Map{"databases": dbs})
	})

	router.Get("/create-db", func(c *fiber.Ctx) error {
		_, dbID, err := getDB("", true)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "created", "dbID": dbID})
	})

	router.Get("/collections", func(c *fiber.Ctx) error {
		dbID := c.Query("dbID")
		if dbID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "dbID required"})
		}
		db, _, err := getDB(dbID, false)
		if err != nil {
			return c.Status(404).JSON(fiber.Map{"error": err.Error()})
		}
		names, _ := db.GetAllCollections()
		return c.JSON(fiber.Map{"collections": names})
	})

	router.Post("/create-collection", func(c *fiber.Ctx) error {
		var body struct {
			DBID  string `json:"dbID"`
			Name  string `json:"name"`
			Order int    `json:"order"`
		}
		if err := c.BodyParser(&body); err != nil || body.DBID == "" || body.Name == "" || body.Order < 3 {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "dbID, name and order>=3 required"})
		}

		db, _, err := getDB(body.DBID, false)
		if err != nil {
			return c.Status(fiber.StatusInternalServerError).JSON(fiber.Map{"error": err.Error()})
		}
		if err := db.CreateCollection(body.Name, body.Order); err != nil {
			return c.Status(fiber.StatusConflict).JSON(fiber.Map{"error": err.Error()})
		}
		return c.JSON(fiber.Map{"status": "collection created"})
	})

	router.Post("/insert", func(c *fiber.Ctx) error {
		var body struct {
			DBID       string `json:"dbID"`
			Collection string `json:"collection"`
			Key        string `json:"key"`
			Value      string `json:"value"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json"})
		}

		db, _, err := getDB(body.DBID, false)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll, err := db.GetCollection(body.Collection)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll.InsertKV(body.Key, body.Value)
		return c.JSON(fiber.Map{"status": "inserted"})
	})

	router.Get("/find", func(c *fiber.Ctx) error {
		dbID, colName, key := c.Query("dbID"), c.Query("collection"), c.Query("key")
		if dbID == "" || colName == "" || key == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing query params"})
		}

		db, _, err := getDB(dbID, false)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll, err := db.GetCollection(colName)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		val, found := coll.FindKey(key)
		if !found {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": "key not found"})
		}
		return c.JSON(fiber.Map{"value": val})
	})

	router.Get("/find-all", func(c *fiber.Ctx) error {
		dbID, colName := c.Query("dbID"), c.Query("collection")

		db, _, err := getDB(dbID, false)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll, err := db.GetCollection(colName)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		val := coll.FindAllKV()

		return c.JSON(fiber.Map{"value": val})
	})

	router.Get("/snapshots", func(c *fiber.Ctx) error {
		dbName := c.Query("dbID")
		basePath := filepath.Join(".", "files", dbName)

		cwd, _ := os.Getwd()
		if !strings.HasSuffix(cwd, basePath) {
			if err := os.Chdir(basePath); err != nil {
				fmt.Fprintf(os.Stderr, "Error changing directory to %s: %v\n", basePath, err)
			}
		}
		snapshots, err := dbcli.LoadSnapshots()
		if err != nil {
			fmt.Fprintf(os.Stderr, "Error loading snapshots: %v\n", err)
			os.Exit(1)
		}

		if len(snapshots) == 0 {
			fmt.Fprintf(os.Stderr, "No snapshots found.\n")
			// os.Exit(1)
		}

		// Convert map to slice for sorting.
		type snapEntry struct {
			Key      string
			Snapshot dbcli.Snapshot
		}
		var snapshotList []snapEntry
		for key, snap := range snapshots {
			snapshotList = append(snapshotList, snapEntry{Key: key, Snapshot: snap})
		}

		// Sort snapshots by timestamp.
		// If timestamps cannot be parsed, fallback to a simple string comparison.
		sort.Slice(snapshotList, func(i, j int) bool {
			ti, err1 := time.Parse(time.RFC3339, snapshotList[i].Snapshot.Timestamp)
			tj, err2 := time.Parse(time.RFC3339, snapshotList[j].Snapshot.Timestamp)
			if err1 != nil || err2 != nil {
				return snapshotList[i].Snapshot.Timestamp < snapshotList[j].Snapshot.Timestamp
			}
			return ti.Before(tj)
		})

		os.Chdir("../..")

		return c.Status(200).JSON(fiber.Map{"snapshots": snapshotList})

	})

	router.Post("/update", func(c *fiber.Ctx) error {
		var body struct {
			DBID       string `json:"dbID"`
			Collection string `json:"collection"`
			Key        string `json:"key"`
			Value      string `json:"value"`
		}
		if err := c.BodyParser(&body); err != nil {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "invalid json"})
		}
		db, _, err := getDB(body.DBID, false)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll, err := db.GetCollection(body.Collection)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll.UpdateKV(body.Key, body.Value)
		return c.JSON(fiber.Map{"status": "updated"})
	})

	router.Delete("/delete", func(c *fiber.Ctx) error {
		dbID, colName, key := c.Query("dbID"), c.Query("collection"), c.Query("key")
		if dbID == "" || colName == "" || key == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "missing query params"})
		}

		db, _, err := getDB(dbID, false)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll, err := db.GetCollection(colName)
		if err != nil {
			return c.Status(fiber.StatusNotFound).JSON(fiber.Map{"error": err.Error()})
		}
		coll.DeleteKey(key)
		return c.JSON(fiber.Map{"status": "deleted (if key existed)"})
	})

	// nutella-style routes
	router.Post("/init", func(c *fiber.Ctx) error {
		var b struct {
			DBID string `json:"dbID"`
		}
		if err := c.BodyParser(&b); err != nil || b.DBID == "" {
			return c.Status(fiber.StatusBadRequest).JSON(fiber.Map{"error": "dbID required"})
		}
		out, err := runCLI([]string{"init", b.DBID})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error(), "output": out})
		}
		return c.JSON(fiber.Map{"output": out})
	})

	router.Post("/commit-all", func(c *fiber.Ctx) error {
		var b struct{ DBID, Message string }
		if err := c.BodyParser(&b); err != nil || b.DBID == "" || b.Message == "" {
			return c.Status(400).JSON(fiber.Map{"error": "dbID and message required"})
		}
		out, err := runCLI([]string{"commit-all", b.DBID, "-m", b.Message})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error(), "output": out})
		}
		return c.JSON(fiber.Map{"output": out})
	})

	router.Post("/restore", func(c *fiber.Ctx) error {
		var b struct{ DBID string }
		if err := c.BodyParser(&b); err != nil || b.DBID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "dbID required"})
		}
		out, err := runCLI([]string{"restore", b.DBID})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error(), "output": out})
		}
		delete(openDBs, b.DBID)
		return c.JSON(fiber.Map{"output": out})
	})

	router.Post("/restore-to", func(c *fiber.Ctx) error {
		var b struct {
			DBID        string `json:"dbID"`
			Commit_hash string `json:"commit_hash"`
		}
		if err := c.BodyParser(&b); err != nil || b.DBID == "" || b.Commit_hash == "" {
			return c.Status(400).JSON(fiber.Map{"error": "DBID required"})
		}
		out, err := runCLI([]string{"restore-to", b.DBID, b.Commit_hash})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error(), "output": out})
		}
		delete(openDBs, b.DBID)
		return c.JSON(fiber.Map{"output": out})
	})

	router.Post("/pack", func(c *fiber.Ctx) error {
		var b struct{ DBID string }
		if err := c.BodyParser(&b); err != nil || b.DBID == "" {
			return c.Status(400).JSON(fiber.Map{"error": "dbID required"})
		}
		out, err := runCLI([]string{"pack", b.DBID})
		if err != nil {
			return c.Status(500).JSON(fiber.Map{"error": err.Error(), "output": out})
		}
		return c.JSON(fiber.Map{"output": out})
	})
}
