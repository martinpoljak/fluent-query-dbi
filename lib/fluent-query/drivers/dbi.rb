# encoding: utf-8
require "abstract"
require "hash-utils/object"   # >= 0.17.0
require "hash-utils/array"

require "fluent-query/drivers/sql"
require "fluent-query/drivers/exception"
require "fluent-query/drivers/shared/results/dbi"
require "fluent-query/drivers/dbi/prepared"

module FluentQuery
    module Drivers

         ##
         # Generic DBI database driver.
         # @abstract
         #
         
         class DBI < FluentQuery::Drivers::SQL
         
            ##
            # Constructor.
            #

            public
            def initialize(connection)
                if self.instance_of? DBI
                    not_implemented
                end

                super(connection)
            end
            
            ##
            # Returns preparation placeholder.
            #
            
            public
            def quote_placeholder
                "?"
            end
         
            ##### EXECUTING
            
            ##
            # Builds connection string according to settings.
            # @return [String] connection string
            #
            
            public
            def connection_string   
                         
                if @_nconnection_settings.nil?
                    raise FluentQuery::Drivers::Exception::new('Connection settings hasn\'t been assigned yet.')
                end
                
                # Gets settings
                
                server = @_nconnection_settings[:server]
                port = @_nconnection_settings[:port]
                socket = @_nconnection_settings[:socket]
                database = @_nconnection_settings[:database]
                
                # Builds connection string and other parameters
                
                if server.nil?
                    server = "localhost"
                end
                
                connection_string = "DBI:%s:database=%s;host=%s" % [self.driver_name, database, server]
                
                if not port.nil?
                    connection_string << ";port=" << port.to_s
                end
                if not socket.nil?
                    connection_string << ";socket=" << socket
                end

                # Returns 
                return connection_string
                
            end
            
            ##
            # Returns DBI driver name.
            #
            # @return [String] driver name
            # @abstract
            #
            
            public
            def driver_name
                not_implemented
            end
                        
            ##
            # Returns authentification settings.
            # @return [Array] with username and password
            #
            
            public
            def authentification
                @_nconnection_settings.take_values(:username, :password)
            end

            ##
            # Opens the connection.
            #
            # It's lazy, so it will open connection before first request through
            # {@link native_connection()} method.
            #

            public
            def open_connection(settings)
                @_nconnection_settings = settings
            end

            ##
            # Returns native connection.
            #

            public
            def native_connection

                if not @_nconnection_settings
                    raise FluentQuery::Drivers::Exception::new("Connection is closed.")
                end
            
                if @_nconnection.nil?
                    require "dbi"
                    
                    # Gets authentification
                    username, password = self.authentification

                    # Connects
                    @_nconnection = ::DBI::connect(self.connection_string, username, password)
                end

                return @_nconnection                    
            end

            ##
            # Closes the connection.
            #

            public
            def close_connection!
                if not @_nconnection.nil?
                    @_nconnection.disconnect
                end
            end

            ##
            # Executes the query and returns data result.
            #

            public
            def execute(query)
                connection = self.native_connection
                
                if query.array? and query.first.kind_of? FluentQuery::Drivers::DBI::Prepared
                    data = query.first.execute(*query.second)
                else
                    data = connection.execute(query.to_s)
                end
                
                return FluentQuery::Drivers::Shared::Results::DBI::new(data)
            end

            ##
            # Executes the query and returns count of the changed/inserted rows.
            #

            public
            def do(query)
                connection = self.native_connection
                count = connection.do(query)

                return count
            end
            
            ##
            # Generates prepared query.
            #
            
            public
            def prepare(query)
                DBI::Prepared::new(self, query)
            end
            
        end
    end
end

