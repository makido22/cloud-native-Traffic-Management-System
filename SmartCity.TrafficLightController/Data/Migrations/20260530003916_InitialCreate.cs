using System;
using Microsoft.EntityFrameworkCore.Migrations;
using Npgsql.EntityFrameworkCore.PostgreSQL.Metadata;

#nullable disable

#pragma warning disable CA1814 // Prefer jagged arrays over multidimensional

namespace SmartCity.TrafficLightController.Data.Migrations
{
    /// <inheritdoc />
    public partial class InitialCreate : Migration
    {
        /// <inheritdoc />
        protected override void Up(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.CreateTable(
                name: "intersections",
                columns: table => new
                {
                    Id = table.Column<int>(type: "integer", nullable: false),
                    Name = table.Column<string>(type: "text", nullable: false),
                    CurrentColor = table.Column<string>(type: "text", nullable: false, defaultValue: "RED"),
                    Direction = table.Column<string>(type: "text", nullable: false),
                    Mode = table.Column<string>(type: "text", nullable: false, defaultValue: "Normal"),
                    EmergencyLockUntil = table.Column<DateTime>(type: "timestamp with time zone", nullable: true),
                    LastChangeReason = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: true),
                    LastUpdatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    CreatedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false),
                    xmin = table.Column<uint>(type: "xid", rowVersion: true, nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_intersections", x => x.Id);
                });

            migrationBuilder.CreateTable(
                name: "state_change_logs",
                columns: table => new
                {
                    Id = table.Column<long>(type: "bigint", nullable: false)
                        .Annotation("Npgsql:ValueGenerationStrategy", NpgsqlValueGenerationStrategy.IdentityAlwaysColumn),
                    IntersectionId = table.Column<int>(type: "integer", nullable: false),
                    PreviousColor = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    NewColor = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    Direction = table.Column<string>(type: "character varying(20)", maxLength: 20, nullable: false),
                    Reason = table.Column<string>(type: "character varying(200)", maxLength: 200, nullable: false),
                    WasEmergency = table.Column<bool>(type: "boolean", nullable: false),
                    LatencyMs = table.Column<double>(type: "double precision", nullable: false),
                    ChangedAt = table.Column<DateTime>(type: "timestamp with time zone", nullable: false)
                },
                constraints: table =>
                {
                    table.PrimaryKey("PK_state_change_logs", x => x.Id);
                    table.ForeignKey(
                        name: "FK_state_change_logs_intersections_IntersectionId",
                        column: x => x.IntersectionId,
                        principalTable: "intersections",
                        principalColumn: "Id",
                        onDelete: ReferentialAction.Cascade);
                });

            migrationBuilder.InsertData(
                table: "intersections",
                columns: new[] { "Id", "CreatedAt", "CurrentColor", "Direction", "EmergencyLockUntil", "LastChangeReason", "LastUpdatedAt", "Mode", "Name" },
                values: new object[,]
                {
                    { 101, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-101" },
                    { 102, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-102" },
                    { 103, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-103" },
                    { 104, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-104" },
                    { 105, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-105" },
                    { 106, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-106" },
                    { 107, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-107" },
                    { 108, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-108" },
                    { 109, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-109" },
                    { 110, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-110" },
                    { 111, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-111" },
                    { 112, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-112" },
                    { 113, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-113" },
                    { 114, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-114" },
                    { 115, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-115" },
                    { 116, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-116" },
                    { 117, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-117" },
                    { 118, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-118" },
                    { 119, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-119" },
                    { 120, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "RED", "Northbound", null, null, new DateTime(2024, 1, 1, 0, 0, 0, 0, DateTimeKind.Utc), "Normal", "Intersection-120" }
                });

            migrationBuilder.CreateIndex(
                name: "IX_intersections_LastUpdatedAt",
                table: "intersections",
                column: "LastUpdatedAt");

            migrationBuilder.CreateIndex(
                name: "IX_intersections_Mode",
                table: "intersections",
                column: "Mode");

            migrationBuilder.CreateIndex(
                name: "IX_state_change_logs_ChangedAt",
                table: "state_change_logs",
                column: "ChangedAt");

            migrationBuilder.CreateIndex(
                name: "IX_state_change_logs_IntersectionId_ChangedAt",
                table: "state_change_logs",
                columns: new[] { "IntersectionId", "ChangedAt" });
        }

        /// <inheritdoc />
        protected override void Down(MigrationBuilder migrationBuilder)
        {
            migrationBuilder.DropTable(
                name: "state_change_logs");

            migrationBuilder.DropTable(
                name: "intersections");
        }
    }
}
