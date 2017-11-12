#pragma once

#include <unordered_set>
#include <vector>
#include <algorithm>
#include <assert.h>
#include <deque>
namespace MTObjects
{
using std::unordered_set;
using std::vector;
using std::deque;

struct IThreadSafeObject
{
private:
	friend struct Cluster;
	Cluster* cluster_ = nullptr;

public:
	virtual void IsDependentOn(vector<IThreadSafeObject*>& ref_dependencies) const = 0;
	virtual void IsConstDependentOn(vector<const IThreadSafeObject*>& ref_dependencies) const = 0;
};

struct Cluster
{
	vector<IThreadSafeObject*> objects_;			  //with duplicates
	vector<const IThreadSafeObject*> const_dependencies_; //with duplicates
	unordered_set<const Cluster*> const_dependencies_clusters_;

	int index = -1;

	void Reset()
	{
		objects_.clear();
		const_dependencies_.clear();
		const_dependencies_clusters_.clear();
		index = -1;
	}

	Cluster()
	{
		objects_.reserve(32);
		const_dependencies_.reserve(32);
		const_dependencies_clusters_.reserve(32);
		Reset();
	}

private:

	void MergeTo(Cluster& main_cluster)
	{
		for (auto obj : objects_)
		{
			obj->cluster_ = &main_cluster;
		}
		main_cluster.objects_.insert(main_cluster.objects_.end(), objects_.begin(), objects_.end());
		main_cluster.const_dependencies_.insert(main_cluster.const_dependencies_.end(), const_dependencies_.begin(), const_dependencies_.end());

		//At this stage const_dependencies_clusters_ are not generated yet
	}

	static void RemoveCluster(vector<Cluster*> &clusters, const Cluster* cluster)
	{
		const int last_index = clusters.size() - 1;
		const int index_to_reuse = cluster->index;
		if (last_index != index_to_reuse)
		{
			Cluster* const moved_cluster = clusters[last_index];
			clusters[index_to_reuse] = moved_cluster;
			moved_cluster->index = index_to_reuse;
		}
		clusters.pop_back();
	}

	void GatherObjects(IThreadSafeObject* in_obj, int& object_counter, vector<Cluster*>& clusters_to_merge, vector<IThreadSafeObject*>& objects_to_handle)
	{
		objects_to_handle.clear();
		objects_to_handle.push_back(in_obj);
		while (!objects_to_handle.empty())
		{
			IThreadSafeObject* obj = objects_to_handle.back();
			objects_to_handle.pop_back();
			if (nullptr == obj->cluster_)
			{
				objects_.emplace_back(obj);
				obj->cluster_ = this;
				obj->IsConstDependentOn(const_dependencies_);
				obj->IsDependentOn(objects_to_handle);
				object_counter--;
			}
			else if (obj->cluster_ != this)
			{
				clusters_to_merge.emplace_back(obj->cluster_);
			}
		}
	}

	static void CreateClusters(const vector<IThreadSafeObject *> &all_objects, vector<Cluster*> &clusters, deque<Cluster>& preallocated_clusters)
	{
		vector<Cluster*> clusters_to_merge;
		clusters_to_merge.reserve(128);
		vector<IThreadSafeObject*> objects_buff;
		objects_buff.reserve(128);
		int objects_counter = all_objects.size();
		unsigned int cluster_counter = 0;
		int first_remaining_obj_index = -1;
		while (objects_counter > 0)
		{
			assert(cluster_counter < preallocated_clusters.size());
			Cluster* cluster = &preallocated_clusters[cluster_counter];
			do { first_remaining_obj_index++; } while (nullptr != all_objects[first_remaining_obj_index]->cluster_);
			cluster->GatherObjects(all_objects[first_remaining_obj_index], objects_counter, clusters_to_merge, objects_buff);
			if (clusters_to_merge.empty())
			{
				cluster->index = clusters.size();
				clusters.emplace_back(cluster);
				cluster_counter++;
			}
			else
			{
				Cluster* main_cluster = clusters_to_merge[0];
				cluster->MergeTo(*main_cluster);
				cluster->Reset();
				const int num_clusters_to_merge = clusters_to_merge.size();
				for (int i = 1; i < num_clusters_to_merge; i++)
				{
					Cluster* redundant_cluster = clusters_to_merge[i];
					if ((redundant_cluster->index != -1) && (redundant_cluster != main_cluster))
					{
						redundant_cluster->MergeTo(*main_cluster);
						RemoveCluster(clusters, redundant_cluster);
						redundant_cluster->index = -1;
					}
				}
				clusters_to_merge.clear();
			}
		}
	}

	static void CreateClustersDependencies(const vector<Cluster*>& clusters)
	{
		vector < const IThreadSafeObject*> const_dependencies;
		const_dependencies.reserve(1024);
		for (auto cluster : clusters)
		{
			//Generate cluster dependencies. It can e done, only when all objects have cluster set.
			for (const IThreadSafeObject* obj : cluster->const_dependencies_)
			{
				if (obj->cluster_ != cluster)
				{
					cluster->const_dependencies_clusters_.emplace(obj->cluster_);
				}
			}
			//cluster->const_dependencies_.clear();
			//cluster->const_dependencies_.shrink_to_fit();

			//Remove duplicates:
			//cluster->unique_objects_.insert(cluster->objects_.begin(), cluster->objects_.end());
			//cluster->objects_.clear();
			//cluster->objects_.shrink_to_fit();
		}
	}

public:

	static void ClearClustersInObjects(const vector<IThreadSafeObject*>& all_objects)
	{
		for (auto obj : all_objects)
		{
			obj->cluster_ = nullptr;
		}
	}

	static vector<Cluster*> GenerateClusters(const vector<IThreadSafeObject*>& all_objects, deque<Cluster>& preallocated_clusters)
	{
		vector<Cluster*> clusters;
		clusters.reserve(preallocated_clusters.size());
		CreateClusters(all_objects, clusters, preallocated_clusters);
		CreateClustersDependencies(clusters);
		return clusters;
	}

	static bool Test_AreClustersCoherent(const vector<Cluster*>& clusters)
	{
		// All dependencies of the objects must be inside the cluster
		for (auto cluster : clusters)
		{
			for (auto obj : cluster->objects_)
			{
				assert(obj && obj->cluster_ == cluster);

				vector<IThreadSafeObject*> dependencies;
				obj->IsDependentOn(dependencies);
				for (auto dep : dependencies)
				{
					assert(dep && dep->cluster_ == cluster);
				}
			}
		}
		return true;
	}
};

struct GroupOfConcurrentClusters : public vector<Cluster*>
{
	unordered_set<const Cluster*> const_dependencies_clusters_;

	void AddCluster(Cluster& cluster)
	{
		emplace_back(&cluster);
		const_dependencies_clusters_.insert(cluster.const_dependencies_clusters_.begin(), cluster.const_dependencies_clusters_.end());
	}

	static vector<GroupOfConcurrentClusters> GenerateClusterGroups(const vector<Cluster*>& clusters)
	{
		auto depends_on = [](const Cluster* a, const Cluster* b) -> bool
		{
			return a->const_dependencies_clusters_.end() != a->const_dependencies_clusters_.find(b);
		};

		auto fits_into_group = [&](const GroupOfConcurrentClusters& group, const Cluster* cluster) -> bool
		{
			return std::none_of(group.begin(), group.end(), [&](const Cluster* cluster_in_group)
			{
				return depends_on(cluster, cluster_in_group);
			}) 

			&& group.const_dependencies_clusters_.find(cluster) == group.const_dependencies_clusters_.end();
		};

		vector<GroupOfConcurrentClusters> groups;
		groups.reserve(clusters.size());
		groups.resize(std::max<int>(1, clusters.size() / 32));
		int cluster_index = 0;
		for (auto cluster : clusters)
		{
			bool fits_in_existing_group = false;
			{
				const int group_num = groups.size();
				const int first_group_to_try = cluster_index % group_num;
				for (int groups_checked = 0; groups_checked < group_num && !fits_in_existing_group; groups_checked++)
				{
					const int group_idx = (first_group_to_try + groups_checked) % group_num;
					auto& group = groups[group_idx];
					if (fits_into_group(group, cluster))
					{
						group.AddCluster(*cluster);
						fits_in_existing_group = true;
					}
				}
			}
			if (!fits_in_existing_group)
			{
				GroupOfConcurrentClusters new_group;
				new_group.AddCluster(*cluster);
				groups.emplace_back(new_group);
			}
			cluster_index++;
		}
		return groups;
	}
};
}