# encoding: utf-8
require "abstract"

require "fluent-query/drivers/sql"
require "fluent-query/drivers/exception"
require "fluent-query/drivers/shared/results/dbi"

module FluentQuery
    module Drivers

         ##
         # Generic DBI database driver.
         # @abstract
         #
         
         class DBI < FluentQuery::Drivers::SQL

            ##
            # Contructor.
            #

            public
            def initialize(connection)
                if self.instance_of? FluentQuery::Drivers::DBI
                    not_implemented
                end

                super(connection)
            end
         
            ##### EXECUTING
            
            ##
            # Builds connection string according to settings.
            #
            # @return [String] connection string
            # @abstract
            #
            
            public
            def connection_string                
                not_implemented
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
            
                if not @_nconnection
                    require "dbi"

                    # Connects
                    @_nconnection = ::DBI::connect(self.connection_string)
                end

                return @_nconnection                    
            end

            ##
            # Closes the connection.
            #

            public
            def close_connection!
                if @_nconnection
                    @_nconnection.disconnect
                end
            end

            ##
            # Executes the query and returns data result.
            #

            public
            def execute(query)
                connection = self.native_connection
                data = connection.execute(query)
                
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
            # Executes query conditionally.
            #
            # If query isn't suitable for executing, returns it. In otherwise
            # returns result or number of changed rows.
            #
            # @abstract
            #

            public
            def execute_conditionally(query, sym, *args, &block)
                not_implemented
            end
        end
    end
end

