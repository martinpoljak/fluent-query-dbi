# encoding: utf-8
require "abstract"
require "hash-utils/object"   # >= 0.17.0
require "hash-utils/symbol"
require "hash-utils/array"
require "hash-utils/hash"

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
            # Indicates, method is relevant for the driver.
            #
            # @since 0.9.2
            # @abstract
            #

            public
            def relevant_method?(name)
                if name.start_with? "prepare_"
                    _name = name[8..-1].to_sym
                else
                    _name = name
                end
                
                super(_name)
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
                                
            ##
            # Checks query conditionally. It's called after first token
            # of the query.
            #
            # @see FluentQuery::Drivers::SQL#execute_conditionally
            # @see #prepare_conditionally
            # @since 0.9.2
            #
            
            public
            def check_conditionally(query, sym, *args, &block)
                if sym.start_with? "prepare_" 
                    self.prepare_conditionally(query, sym, *args, &block)
                else
                    super(query, sym, *args, &block)
                end
            end
            
            ##
            # Prepares query conditionally.
            #
            # If query isn't suitable for preparing, returns it. In otherwise
            # returns +nil+, result or number of changed rows.
            # 
            # @since 0.9.2
            #

            public
            def prepare_conditionally(query, sym, *args, &block)
                case query.type
                    when :insert
                        if (args.first.symbol?) and (args.second.hash?)
                            result = query.prepare!
                        end
                    when :truncate
                        if args.first.symbol?
                            result = query.prepare!
                        end
                    else
                        result = nil
                end
                
                return result
            end
            
            ##
            # Corrects token before it's pushed to the token. So allows to
            # modify data assigned to the query from driver level.
            #
            # @since 0.9.2
            #
            
            public
            def correct_token(name, args)
                if name.start_with? "prepare_" 
                    name = name[8..-1].to_sym
                end
                
                return [name, args]
            end
            
        end
    end
end

