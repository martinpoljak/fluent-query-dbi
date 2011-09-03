# encoding: utf-8

module FluentQuery
    module Drivers
        class DBI < FluentQuery::Drivers::SQL

            ##
            # DBI prepared query.
            #

            class Prepared
            
                ##
                # Holds associated driver instance.
                #
                
                @driver
                
                ##
                # Holds appropriate query.
                #
                
                @query
                
                ##
                # Holds prepared query in native form.
                #
                
                @native
                
                ##
                # Holds prepared query form.
                #
                
                @prepared
                
                ##
                # Holds directives matcher.
                #
                
                @@matcher = nil
                
                ##
                # Constructor.
                #
                
                def initialize(driver, query)
                    @driver = driver
                    @query = query
                end
                
                ##
                # Returns directives matcher.
                #
                
                def matcher
                    if @@matcher.nil?
                        directives = FluentQuery::Compiler::FORMATTING_DIRECTIVES.map { |s| s.to_s }
                        @@matcher = Regexp::new("%%(?:" << directives.join("|") << ")(?:([^\w])|$)")
                    end
                    
                    @@matcher
                end
                
                ##
                # Returns prepared query form.
                #
                
                def prepared
                    if @prepared.nil?
                        string = @driver.build_query(@query, :prepare)
                        string.gsub!(self.matcher, '?\1')
                        @prepared = string
                    end
#p @prepared
                    @prepared
                end
                
                ##
                # Returns prepared query in the native form.
                #
                
                def native
                    if @native.nil?
                        @native = @driver.native_connection.prepare(self.prepared)
                    end
                    
                    @native
                end
                
                
                ##
                # Executes the query.
                #
                
                def execute(*args)
                    self.native.execute(*args)
                    self.native
                end
                
            end
            
        end
    end
end

