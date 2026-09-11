package main

import (
	"embed"
	"log"
	"time"

	"github.com/okuu/kafka_kaitai/services"
	"github.com/wailsapp/wails/v3/pkg/application"
)

//go:embed all:frontend/dist
var assets embed.FS

func init() {
	application.RegisterEvent[string]("time")
	application.RegisterEvent[services.SystemTelemetry]("telemetry")
}

func main() {
	kaitaiService, err := services.NewKaitaiService()
	if err != nil {
		log.Printf("Warning: failed to init kaitai service: %v", err)
	}
	kafkaService := services.NewKafkaService(kaitaiService)

	schemaService := services.NewSchemaRegistryService()
	metricsService := services.NewMetricsService()

	app := application.New(application.Options{
		Name:        "KafkaKaitai",
		Description: "High-Performance Kafka Client with Kaitai Struct Binary Parsing",
		Services: []application.Service{
			application.NewService(kafkaService),
			application.NewService(kaitaiService),
			application.NewService(schemaService),
			application.NewService(metricsService),
		},
		Assets: application.AssetOptions{
			Handler: application.AssetFileServerFS(assets),
		},
		Mac: application.MacOptions{
			ApplicationShouldTerminateAfterLastWindowClosed: true,
		},
	})

	app.Window.NewWithOptions(application.WebviewWindowOptions{
		Title:  "KafkaKaitai — High-Performance Binary Kafka Client",
		Width:  1400,
		Height: 900,
		Mac: application.MacWindow{
			InvisibleTitleBarHeight: 50,
			Backdrop:                application.MacBackdropTranslucent,
			TitleBar:                application.MacTitleBarHiddenInset,
		},
		BackgroundColour: application.NewRGB(15, 23, 42), // Slate-900 dark theme
		URL:              "/",
	})

	// Background telemetry ticker
	go func() {
		ticker := time.NewTicker(time.Second)
		defer ticker.Stop()
		for range ticker.C {
			now := time.Now().Format("15:04:05")
			app.Event.Emit("time", now)
			app.Event.Emit("telemetry", metricsService.GetSystemTelemetry())
		}
	}()

	if err := app.Run(); err != nil {
		log.Fatal(err)
	}
}
