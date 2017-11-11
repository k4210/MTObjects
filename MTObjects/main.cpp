#include "IThreadSafeObject.h"
#include <vector>
#include <algorithm>
#include <random>
#include <iostream>
using namespace MTObjects;
using std::vector;

class TestObject : public IThreadSafeObject
{
public:
	vector<TestObject*> dependencies_;
	vector<const TestObject*> const_dependencies_;

	int id_ = -1;
	int counter_ = 0;

	vector<IThreadSafeObject*> IsDependentOn() const override
	{
		return vector<IThreadSafeObject*>(dependencies_.begin(), dependencies_.end());
	}

	void IsConstDependentOn(unordered_set<const	IThreadSafeObject*>& ref_dependencies) const
	{
		ref_dependencies.insert(const_dependencies_.begin(), const_dependencies_.end());
	}

	void Task()
	{
		for (auto obj : const_dependencies_)
		{
			counter_ += obj->counter_;
		}

		for (auto obj : dependencies_)
		{
			obj->counter_ -= counter_;
		}
	}
};

void main()
{
	std::cout << "num_objects: ";
	int num_objects = 2048;
	std::cin >> num_objects; 

	std::cout << "dependencies_num: ";
	int dependencies_num = 3;
	std::cin >> dependencies_num;

	std::cout << "const_dependencies_num: ";
	int const_dependencies_num = 3;
	std::cin >> const_dependencies_num;
	

	std::default_random_engine generator;
	std::uniform_int_distribution<int> distribution(0, num_objects-1);

	{
		vector<TestObject> vec_obj;
		vec_obj.resize(num_objects);
		TestObject* obj = &vec_obj[0];
		for (int i = 0; i < num_objects; i++)
		{
			obj[i].id_ = i;
			for (int j = 0; j < dependencies_num; j++)
			{
				obj[i].dependencies_.emplace_back(obj + distribution(generator));
			}
			for (int j = 0; j < dependencies_num; j++)
			{
				obj[i].const_dependencies_.emplace_back(obj + distribution(generator));
			}
		}

		unordered_set<IThreadSafeObject*> all_objects;
		all_objects.reserve(num_objects);
		for (int i = 0; i < num_objects; i++)
		{
			all_objects.emplace(obj + i);
		}

		const unordered_set<shared_ptr<Cluster>> clusters = Cluster::GenerateClasters(all_objects);
		std::cout << "num_objects: " << num_objects << " dependencies_num: " << dependencies_num << std::endl;
		std::cout << "clusters: " << clusters.size() << std::endl;

		const vector<GroupOfConcurrentClusters> groups = GroupOfConcurrentClusters::GenerateClasterGroups(clusters);
		std::cout << "groups: " << groups.size() << std::endl;

		for (unsigned int group_idx = 0; group_idx < groups.size(); group_idx++)
		{
			const GroupOfConcurrentClusters& group = groups[group_idx];
			std::cout << group_idx << "[" << group.size() << "]\t ";
			for (unsigned int cluster_idx = 0; cluster_idx < group.size(); cluster_idx++)
			{
				auto& cluster = group[cluster_idx];
				std::cout << cluster->objects_.size() 
					//<< "(" << cluster->const_dependencies_clusters_.size() << ")"
					<< " ";
			}
			std::cout << std::endl;
		}
	}
	getchar();
	getchar();
}