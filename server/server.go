package server

import (
	routes "db/server/routes"
	"log"

	"github.com/gofiber/fiber/v2"
	"github.com/spf13/cobra"
)

func Server(cmd *cobra.Command) {
	app := fiber.New()

	routes.SetupRoutes(app)

	log.Println("Fiber listening on :3000")
	if err := app.Listen(":3000"); err != nil {
		log.Fatal(err)
	}
}
