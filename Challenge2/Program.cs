using Npgsql;
using System;

class Program
{
   
    private static string connectionString =
        "Host=totally-exultant-steelhead.data-1.use1.tembo.io;" +
        "Username=postgres;" +
        "Password=7E7sk18WZl96qeuj;" +
        "Database=postgres";

    static void Main(string[] args)
    {
        
        using var connection = new NpgsqlConnection(connectionString);
        connection.Open();
        Console.WriteLine("Connected to PostgreSQL");

        
        REPL(connection);
    }

    
    static void REPL(NpgsqlConnection connection)
    {
        while (true)
        {
            Console.Write("sql> ");
            var input = Console.ReadLine();

            
            if (input.ToLower() == "exit") break;

            try
            {
               
                if (input.StartsWith("\\t "))
                {
                    string[] parts = input.Split(' ');
                    if (parts.Length == 2)
                    {
                        ShowTableInfo(connection, parts[1]); 
                    }
                    else
                    {
                        Console.WriteLine("Invalid command. Use: \\t <table_name>");
                    }
                }
                else
                {
                    
                    ExecuteQuery(connection, input);
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error: {ex.Message}");
            }
        }
    }

    
    static void ExecuteQuery(NpgsqlConnection connection, string query)
    {
        using var cmd = new NpgsqlCommand(query, connection);
        using var reader = cmd.ExecuteReader();

        
        if (reader.HasRows)
        {
            
            for (int i = 0; i < reader.FieldCount; i++)
            {
                Console.Write($"{reader.GetName(i),-20}");
            }
            Console.WriteLine();

            
            while (reader.Read())
            {
                for (int i = 0; i < reader.FieldCount; i++)
                {
                    Console.Write($"{reader.GetValue(i),-20}");
                }
                Console.WriteLine();
            }
        }
        else
        {
           
            Console.WriteLine("Query executed successfully.");
        }
    }

    
    static void ShowTableInfo(NpgsqlConnection connection, string tableName)
    {
        var query = $@"
            SELECT column_name, data_type, is_nullable, column_default
            FROM information_schema.columns
            WHERE table_name = '{tableName}'";

        using var cmd = new NpgsqlCommand(query, connection);
        using var reader = cmd.ExecuteReader();

        Console.WriteLine($"Columns in {tableName}:");
        Console.WriteLine($"{"Column",-20} {"Data Type",-20} {"Nullable",-10} {"Default",-10}");

        
        while (reader.Read())
        {
            Console.WriteLine($"{reader.GetString(0),-20} {reader.GetString(1),-20} {reader.GetString(2),-10} {reader.GetValue(3),-10}");
        }
    }
}
