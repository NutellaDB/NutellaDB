package server

import (
	routes "db/server/routes"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/gofiber/fiber/v2/middleware/cors"
	"github.com/spf13/cobra"
)

func Server(cmd *cobra.Command) {
	app := fiber.New()
	app.Use(cors.New())

	routes.SetupRoutes(app)

	log.Println("Fiber listening on :3000")
	if err := app.Listen(":3000"); err != nil {
		log.Fatal(err)
	}
}
