require "spec/helper/all"

describe EM::Mongo do

  it "should yield until connection is ready" do
    EventMachine.synchrony do
      connection = EM::Mongo::Connection.new
      connection.connected?.should be_true

      db = connection.db('db')
      db.is_a?(EventMachine::Mongo::Database).should be_true

      EventMachine.stop
    end
  end

  it "should insert a record into db" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      obj = collection.insert('hello' => 'world')
      obj.should be_a(BSON::ObjectId)

      obj = collection.find.to_a
      obj.size.should == 1
      obj.first['hello'].should == 'world'

      EventMachine.stop
    end
  end

  it "should insert a record into db when :safe => true" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      obj = collection.insert('hello' => 'world', :safe => true)
      obj.should be_a(BSON::ObjectId)

      obj = collection.find.to_a
      obj.size.should == 1
      obj.first['hello'].should == 'world'

      EventMachine.stop
    end
  end

  it "should insert a record into db and be able to find it" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      obj = collection.insert('hello' => 'world')
      obj = collection.insert('hello2' => 'world2')

      obj = collection.find({}).to_a
      obj.size.should == 2

      obj2 = collection.find({}, {:limit => 1}).to_a
      obj2.size.should == 1

      obj3 = collection.first
      obj3.is_a?(Hash).should be_true

      EventMachine.stop
    end
  end

  it "should be able to order results" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      collection.insert(:name => 'one', :position => 0)
      collection.insert(:name => 'three', :position => 2)
      collection.insert(:name => 'two', :position => 1)

      res = collection.find({}, {:order => 'position'}).to_a
      res[0]["name"].should == 'one'
      res[1]["name"].should == 'two'
      res[2]["name"].should == 'three'

      res1 = collection.find({}, {:order => [:position, :desc]}).to_a
      res1[0]["name"].should == 'three'
      res1[1]["name"].should == 'two'
      res1[2]["name"].should == 'one'

      EventMachine.stop
    end
  end

  it "should update records in db" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      obj_id = collection.insert('hello' => 'world')
      collection.update({'hello' => 'world'}, {'hello' => 'newworld'})

      new_obj = collection.first({'_id' => obj_id})
      new_obj['hello'].should == 'newworld'

      EventMachine.stop
    end
  end

  it "should find and modify a document" do
    EventMachine.synchrony do
      collection = EM::Mongo::Connection.new.db('db').collection('test')
      collection.remove({}) # nuke all keys in collection

      collection << { :a => 1, :processed => false }
      collection << { :a => 2, :processed => false }
      collection << { :a => 3, :processed => false }

      resp = collection.find_and_modify(:query => {}, :sort => [['a', -1]], :update => {"$set" => {:processed => true}})
      resp['processed'].should_not be_true
      collection.find_one({:a=>3})['processed'].should be_true

      EventMachine.stop
    end
  end

end
