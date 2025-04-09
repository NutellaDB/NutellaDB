package main

import (
    "bytes"
    "fmt"
    "log"
    "net/http"

    "github.com/gofiber/fiber/v2"
    "db/cli"
)

func runCLI(args []string) (string, error) {
    var stdout bytes.Buffer
    var stderr bytes.Buffer

    cli.RootCmd.SetOut(&stdout)
    cli.RootCmd.SetErr(&stderr)
    cli.RootCmd.SetArgs(args)

    err := cli.RootCmd.Execute()
    outStr := stdout.String()
    errStr := stderr.String()

    if err != nil {
        outStr += "\n" + errStr
    }

    return outStr, err
}

func main() {
    app := fiber.New()
	cli.Execute()

    app.Post("/api/create-db", func(c *fiber.Ctx) error {
        output, err := runCLI([]string{"create-db"})
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Create a new collection endpoint
    app.Post("/api/create-collection", func(c *fiber.Ctx) error {
        body := struct {
            DBID        string `json:"dbID"`
            Name        string `json:"name"`
            Order       int    `json:"order"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{
                "error": "Invalid JSON",
            })
        }

        args := []string{
            "create-collection",
            body.DBID,
            body.Name,
            fmt.Sprintf("%d", body.Order),
        }

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Insert key-value endpoint
    app.Post("/api/insert", func(c *fiber.Ctx) error {
        body := struct {
            DBID       string `json:"dbID"`
            Collection string `json:"collection"`
            Key        string `json:"key"`
            Value      string `json:"value"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{
            "insert",
            body.DBID,
            body.Collection,
            body.Key,
            body.Value,
        }

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Find key endpoint
    app.Get("/api/find", func(c *fiber.Ctx) error {
        dbID := c.Query("dbID")
        collName := c.Query("collection")
        key := c.Query("key")

        if dbID == "" || collName == "" || key == "" {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{
                "error": "Missing query params: dbID, collection, key",
            })
        }

        args := []string{
            "find",
            dbID,
            collName,
            key,
        }

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Update key endpoint
    app.Post("/api/update", func(c *fiber.Ctx) error {
        body := struct {
            DBID       string `json:"dbID"`
            Collection string `json:"collection"`
            Key        string `json:"key"`
            Value      string `json:"value"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{
            "update",
            body.DBID,
            body.Collection,
            body.Key,
            body.Value,
        }

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Delete key endpoint
    app.Delete("/api/delete", func(c *fiber.Ctx) error {
        dbID := c.Query("dbID")
        collName := c.Query("collection")
        key := c.Query("key")

        if dbID == "" || collName == "" || key == "" {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{
                "error": "Missing query params: dbID, collection, key",
            })
        }

        args := []string{
            "delete",
            dbID,
            collName,
            key,
        }

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Initialize database endpoint
    app.Post("/api/init", func(c *fiber.Ctx) error {
        body := struct {
            DBID string `json:"dbID"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{"init", body.DBID}
        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Commit all changes endpoint
    app.Post("/api/commit-all", func(c *fiber.Ctx) error {
        body := struct {
            DBID    string `json:"dbID"`
            Message string `json:"message"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{"commit-all", body.DBID, "-m", body.Message}

        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Restore database endpoint
    app.Post("/api/restore", func(c *fiber.Ctx) error {
        body := struct {
            DBID string `json:"dbID"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{"restore", body.DBID}
        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    // Pack database endpoint
    app.Post("/api/pack", func(c *fiber.Ctx) error {
        body := struct {
            DBID string `json:"dbID"`
        }{}
        if err := c.BodyParser(&body); err != nil {
            return c.Status(http.StatusBadRequest).JSON(fiber.Map{"error": "Invalid JSON"})
        }

        args := []string{"pack", body.DBID}
        output, err := runCLI(args)
        if err != nil {
            return c.Status(http.StatusInternalServerError).JSON(fiber.Map{
                "error":  err.Error(),
                "output": output,
            })
        }
        return c.JSON(fiber.Map{"output": output})
    })

    log.Println("Starting Fiber server on port 3000...")
    if err := app.Listen(":3000"); err != nil {
        log.Fatalf("Error starting Fiber: %v", err)
    }
}
